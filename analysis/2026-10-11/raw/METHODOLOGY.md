# Measurement methodology and provenance

## Selection and version boundaries

The packet contains the completed local Lustre/Weka campaign, September NFS and ZFS results, the October 11 corrective local XFS rcp run, the October 10–11 physical two-host remote rcp rerun, and run9's Linux source-tree local and physical remote comparison. Run9 is the selected Linux source result; preliminary run8 overlap is excluded. For the four synthetic XFS filesets, corrective October rcp rows replace September rcp rows, while September cp, dcp and rsync measurements are retained as dated references. The October 10 run5 local XFS measurements were invalidated by the experiment owner and are excluded. The recent remote rcp rerun supersedes the September and October 3 remote rcp rows. September remote rsync is retained only as a dated reference for the synthetic filesets, measured on a different pair of nodes. Those rsync means are not divided by the October rcp means.

The selected campaigns contribute 762 successful rows, plus two right-censored NFS serial-copy outcomes. All selected variants have three successful iterations. Earlier failed attempts, qualification runs, invalidated run5 trials, September XFS rcp measurements, preliminary run8 rows, and superseded remote rcp runs are excluded. `SOURCES.json` records source checksums; `provenance/` retains selected manifests, audits, environment evidence and benchmark runners. Each sample embeds its original ledger record, raw command log and GNU `time -v` output; the October 10–11 synthetic remote rcp run also embeds per-role runtime statistics.

The original source audits report Lustre/Weka 246/246 selected successes, the September compute campaign 354/354 policy-adjusted successes (this packet selects 96 local XFS comparators, 138 NFS successes and 24 remote rsync references), ZFS 144/144, October corrective XFS 48/48, October synthetic remote rcp 48/48, and run9 Linux source 18/18. The corrective XFS and run9 source seals passed SHA256 verification.

## Machines and filesystems

| Cohort | Host or hosts | Storage | Allocation and instance evidence |
| --- | --- | --- | --- |
| Lustre/Weka | `awhpcsubmitccprd1` | source and destination on the same respective mounted filesystem | 91 CPUs in affinity; no dedicated benchmark cgroup; AWS instance type not recorded |
| Local XFS, September references | `compute003200` | source and destination on local `/instance-store` XFS | 32 CPUs and 200G requested; `perf_copy_m8idn_16xl-on-demand` |
| Local XFS, October 11 | `compute003125` | source and destination on local `/instance-store` XFS | 32 CPUs and 200G allocated; Intel Xeon 6975P-C; `perf_copy_m8idn_16xl-on-demand` |
| Linux source local and remote, run9 | `compute009206` local; `compute009206` to `compute005232` remote | XFS `/instance-store` on the source and peer hosts | 32 CPUs and 200G requested per host; `perf_copy_m8idn_16xl-on-demand`; instance IDs `i-03adfcd80458c9c19` and `i-02fd7f0d6947eca0f` |
| NFS, September | `compute003200` | local copy through the `/d` NFS client mount | 32 CPUs and 200G requested; same named Slurm partition |
| ZFS, September | filer `awhpcfilerrcpdev1` | same ZFS device under `/d` | AWS instance type and CPU allocation not recorded |
| Remote rcp, October 10–11 | `compute017153` to `compute017203` | XFS `/instance-store` on separate computes | 32 CPUs and 200G allocated on each; same named Slurm partition |
| Remote rsync reference, September | `compute003200` to `compute027148` | XFS `/instance-store` on separate computes | 32 CPUs and 200G requested per compute; same named Slurm partition |

The compute partition name indicates AWS `m8idn.16xlarge`. The archived evidence does not contain an EC2 instance-type field, so the exact type is inferred from the partition name; run9 does retain instance IDs. Cohorts may differ in host, rcp build, workload and cache procedure; their times are not a controlled filesystem ranking. The September XFS comparator and corrective rcp runs used the same synthetic filesets and cache policy but different nodes and dates. Their rows support descriptive historical comparisons, not a controlled same-run speedup claim.

## Workloads and commands

| Fileset | Files | Directories including root | Apparent bytes | Bytes per file |
| --- | ---: | ---: | ---: | ---: |
| `10x100x1G` | 1,000 | 11 | 1,000,000,000,000 | 1,000,000,000 |
| `10x1024x100M` | 10,240 | 11 | 1,024,000,000,000 | 100,000,000 |
| `10x10x10x1024x1M` | 1,024,000 | 1,111 | 1,024,000,000,000 | 1,000,000 |
| `10x10x10x10x1024x1K` | 10,240,000 | 11,111 | 10,240,000,000 | 1,000 |

Run9 adds the Linux 7.2.9 source tree: 94,775 regular files, 6,204 reported directories, 99 symlinks and 1,616,711,010 apparent bytes measured with `du -sb --apparent-size`. File sizes vary. Its sealed manifest and extractor record source archive SHA256 `ba4996423cf38afc564f795be41d171495344ad6103c35bf2ce62918a1213ec6`. The tarball was not present in the reviewed source copy, so this checksum was not independently reverified for the packet. This workload is separate from the four generated filesets.

For the selected campaigns, `rcp-default` uses automatic settings. Numeric local rcp variants set max files in flight; numeric remote rcp variants set both max files in flight and max connections. `dcp-default` means 32 MPI ranks. `cp` uses `--reflink=auto -a`; `rsync` uses `-a`; their `10x` variants start one process per top-level directory. Run9 uses local default rcp, default 32-rank dcp, serial cp and serial rsync, plus remote default rcp and serial rsync; it has no 10-way or explicit rcp-concurrency variants. Exact commands are in the JSON. Local rcp also defaults to automatic reflink-capable copying. The corrective XFS and run9 local runs did not disable it. On reflink-capable XFS or ZFS, apparent bytes per second can differ greatly from physical storage throughput; physical bytes copied were not recorded. Run9's remote rcp and rsync transfer data between hosts.

## Cache preparation and timing

Lustre/Weka use `sync` followed by `POSIX_FADV_DONTNEED` on each source file. This requests source page-cache eviction but does not clear dentry/inode or storage-server caches. The campaign changed from a Python to a bounded Rust walker, retaining that policy and per-attempt cache evidence. Source staging is untimed.

September local XFS and NFS, October corrective XFS, October synthetic remote compute, and run9 source-host runs issue `sync` then client `drop_caches=3` before every timed copy, followed by a five-second wait. This requests reclamation of that client's page, dentry and inode caches. The XFS and run9 source trees were staged from `/d` to `/instance-store` with `rsync -a` outside timing; file counts and apparent bytes were checked after staging, and run9 also checked the source and staged symlink counts. NFS and other storage-server caches are not controlled; remote destination host cache dropping is not documented. September ZFS uses `sync` and per-file `POSIX_FADV_DONTNEED`; ZFS ARC eviction was not verified.

In the selected campaigns, `copy_seconds` is command wall time. The packet's `completion_seconds` extends from the same start through destination `sync -f` and file-count/apparent-byte validation. For Lustre/Weka this is the original `copy_plus_sync_seconds`; for the September compute, corrective XFS, October synthetic remote, and run9 ledgers it is `sync_seconds`. Lustre/Weka also retains narrower copy-through-sync, sync-only and validation times. Run9's measured rows checked regular-file counts and `du` apparent bytes; their ledger does not record a per-row symlink count or content hash.

GNU `time -v` records user/system CPU and per-process high-water RSS for locally observed processes. For ten-way commands RSS is not a simultaneous total. DCP GNU-time values may primarily describe the MPI launcher; aggregate rank CPU/RSS was not validated. Remote rcp GNU time largely sees the master CLI. Only the October 10–11 synthetic remote rcp run captured separate `--summary` statistics for master, source daemon and destination daemon; run9 did not. Remote rsync excludes receiver-host work. Read CPU and RSS within each accounting scope.

## Censored and excluded rows

The Lustre/Weka selection excludes serial `cp-1x` and `rsync-1x` on the 10.24-million-file set on both filesystems and on Weka's 1.024-million-file set. The NFS policy attempted one sample of each serial tool on its 10.24-million-file set with a 12-hour cap, then omitted iterations 2 and 3. Both attempts hit the cap; their copy times are lower bounds and do not enter means. For synthetic XFS filesets, September cp, dcp and rsync are historical references alongside corrective rcp; run9 supplies a same-run XFS tool comparison for the Linux source fileset.
