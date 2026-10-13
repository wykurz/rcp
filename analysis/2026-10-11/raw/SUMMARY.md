# rcp performance results

762 selected successful measurements in 26 filesystem/fileset JSON files. Each selected variant has three iterations. Every JSON retains raw measurement records and command/resource output. Two NFS serial-copy runs are retained as censored outcomes. The synthetic XFS filesets combine corrective October rcp measurements with dated September cp, dcp and rsync references.

For the selected cohorts, the primary table metric is seconds from copy start through destination sync and validation, with copy-command time shown separately. Read [METHODOLOGY.md](METHODOLOGY.md) for machines, AWS instance-class evidence, cache handling, version boundaries, CPU accounting, and selection rules. [SOURCES.json](SOURCES.json) gives source hashes.

| Local filesystem | Successful samples | rcp build | Source cache handling |
| --- | ---: | --- | --- |
| Lustre | 126 | 0.40.0 / 5a1601f4 | sync + file fadvise |
| Weka | 120 | 0.40.0 / 5a1601f4 | sync + file fadvise |
| XFS | 156 | 0.41.0 / 27c08950 for rcp | sync + client drop_caches=3 |
| NFS | 138 | 0.41.0 / e68a5adf | sync + client drop_caches=3 |
| ZFS | 144 | 0.41.0 / e68a5adf | sync + file fadvise |

For the four synthetic remote filesets, XFS has 48 October rcp samples at build 27c08950 plus 24 dated September rsync reference samples. Those rsync rows used different nodes and dates, so their means have no ratio to October rcp. Run9's Linux source fileset has six current rcp and rsync samples measured on the same two hosts.

Table times and CPU are means over successful iterations. RSS is the largest GNU-time high-water value across iterations. All raw values, including min/max and additional GNU-time fields, are in the linked JSON. DCP CPU/RSS mainly describe the MPI launcher on compute nodes; remote rcp role statistics are embedded in its JSON.

## Local

### Lustre

#### [10x100x1G](local/lustre/10x100x1G.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-32` | 3 | 208.985 | 210.637 | 0.343 | 896.170 | 4608 |
| `rcp-128` | 3 | 213.133 | 214.865 | 0.363 | 1000.517 | 8824 |
| `dcp-32` | 3 | 217.206 | 218.184 | 636.447 | 938.587 | 23380 |
| `dcp-default` | 3 | 217.453 | 218.666 | 668.113 | 940.763 | 23792 |
| `rcp-default` | 3 | 230.589 | 232.330 | 0.337 | 1996.517 | 8836 |
| `rsync-10x` | 3 | 271.627 | 274.132 | 156.910 | 1046.693 | 7944 |
| `cp-10x` | 3 | 274.420 | 277.043 | 0.037 | 735.590 | 3168 |
| `dcp-8` | 3 | 317.042 | 318.023 | 136.207 | 811.693 | 22708 |
| `rcp-8` | 3 | 318.901 | 321.214 | 0.263 | 728.850 | 5912 |
| `rsync-1x` | 3 | 1830.581 | 1832.544 | 143.723 | 824.213 | 7964 |
| `cp-1x` | 3 | 1972.209 | 1974.069 | 0.023 | 570.083 | 2368 |

#### [10x1024x100M](local/lustre/10x1024x100M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-32` | 3 | 217.873 | 220.334 | 2.020 | 1375.393 | 6672 |
| `dcp-32` | 3 | 218.431 | 220.927 | 335.010 | 950.247 | 24376 |
| `rsync-10x` | 3 | 223.433 | 224.946 | 159.577 | 1229.910 | 7976 |
| `cp-10x` | 3 | 235.125 | 237.115 | 0.270 | 919.150 | 3148 |
| `dcp-default` | 3 | 244.017 | 246.022 | 386.890 | 1165.667 | 23732 |
| `rcp-8` | 3 | 269.700 | 272.270 | 1.750 | 912.447 | 6660 |
| `rcp-default` | 3 | 276.820 | 279.231 | 1.977 | 5715.320 | 8528 |
| `rcp-128` | 3 | 281.603 | 283.968 | 1.980 | 3155.100 | 8012 |
| `dcp-8` | 3 | 317.632 | 319.301 | 48.103 | 816.737 | 24044 |
| `rsync-1x` | 3 | 1808.729 | 1810.307 | 149.620 | 965.667 | 7224 |
| `cp-1x` | 3 | 1916.206 | 1917.966 | 0.177 | 688.043 | 2388 |

#### [10x10x10x1024x1M](local/lustre/10x10x10x1024x1M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-32` | 3 | 373.732 | 465.182 | 105.563 | 1899.547 | 104964 |
| `rcp-default` | 3 | 519.931 | 617.599 | 121.187 | 22540.207 | 99632 |
| `rcp-128` | 3 | 530.742 | 619.575 | 112.003 | 26561.190 | 103916 |
| `rsync-10x` | 3 | 747.606 | 835.540 | 195.170 | 1707.493 | 7208 |
| `cp-10x` | 3 | 1317.248 | 1400.068 | 12.470 | 1546.143 | 3172 |
| `rcp-8` | 3 | 1399.677 | 1480.056 | 101.613 | 1779.173 | 107472 |
| `dcp-default` | 3 | 5825.783 | 5904.926 | 2151.890 | 6480.793 | 50632 |
| `dcp-32` | 3 | 5966.359 | 6046.504 | 2325.883 | 5631.580 | 50648 |
| `dcp-8` | 3 | 6844.171 | 6927.540 | 598.760 | 1406.897 | 153320 |
| `rsync-1x` | 3 | 6894.595 | 6967.445 | 182.957 | 1396.680 | 7192 |
| `cp-1x` | 3 | 12126.513 | 12204.456 | 11.487 | 1196.450 | 2412 |

#### [10x10x10x10x1024x1K](local/lustre/10x10x10x10x1024x1K.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-128` | 3 | 1752.414 | 2400.861 | 952.573 | 7890.853 | 1100408 |
| `rcp-default` | 3 | 1728.462 | 2418.960 | 915.603 | 7952.560 | 1103320 |
| `rcp-32` | 3 | 2170.457 | 2853.444 | 863.637 | 6214.343 | 1096612 |
| `rsync-10x` | 3 | 6209.807 | 6862.024 | 124.640 | 4439.817 | 7944 |
| `rcp-8` | 3 | 6949.926 | 7638.197 | 826.180 | 5683.510 | 1097356 |
| `cp-10x` | 3 | 7730.611 | 8443.605 | 78.690 | 3504.160 | 3216 |
| `dcp-default` | 3 | 13694.681 | 14461.362 | 3702.093 | 6705.937 | 391308 |
| `dcp-32` | 3 | 13987.649 | 14795.663 | 6773.630 | 6871.570 | 421040 |
| `dcp-8` | 3 | 20184.329 | 20932.453 | 1216.733 | 4951.220 | 1521512 |

### Weka

#### [10x100x1G](local/weka/10x100x1G.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-32` | 3 | 235.303 | 250.909 | 0.180 | 511.243 | 4608 |
| `rcp-8` | 3 | 254.274 | 271.578 | 0.193 | 500.960 | 4608 |
| `dcp-32` | 3 | 274.983 | 275.286 | 525.943 | 619.743 | 23580 |
| `dcp-default` | 3 | 280.123 | 280.414 | 538.093 | 646.403 | 22904 |
| `rcp-default` | 3 | 263.638 | 280.937 | 0.240 | 511.907 | 4608 |
| `rcp-128` | 3 | 271.078 | 290.247 | 0.240 | 579.503 | 5376 |
| `cp-10x` | 3 | 307.526 | 307.905 | 0.020 | 454.203 | 3152 |
| `dcp-8` | 3 | 324.867 | 325.138 | 75.483 | 604.610 | 22616 |
| `rsync-10x` | 3 | 368.881 | 369.177 | 148.270 | 856.547 | 7968 |
| `cp-1x` | 3 | 1257.417 | 1257.868 | 0.020 | 405.840 | 2436 |
| `rsync-1x` | 3 | 2381.631 | 2381.993 | 144.523 | 813.463 | 7104 |

#### [10x1024x100M](local/weka/10x1024x100M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-8` | 3 | 248.418 | 264.651 | 1.543 | 530.053 | 4608 |
| `rcp-32` | 3 | 254.367 | 268.249 | 1.667 | 553.383 | 4608 |
| `rcp-default` | 3 | 254.777 | 269.503 | 1.953 | 551.437 | 4608 |
| `rcp-128` | 3 | 260.855 | 276.678 | 2.073 | 609.597 | 6664 |
| `dcp-32` | 3 | 277.223 | 277.601 | 173.533 | 748.150 | 22916 |
| `dcp-default` | 3 | 278.656 | 279.045 | 212.143 | 778.957 | 22780 |
| `cp-10x` | 3 | 362.381 | 364.625 | 0.227 | 533.320 | 3168 |
| `dcp-8` | 3 | 493.905 | 494.467 | 69.740 | 632.793 | 24348 |
| `rsync-10x` | 3 | 532.835 | 533.288 | 159.200 | 1043.457 | 7996 |
| `cp-1x` | 3 | 1860.506 | 1862.174 | 0.203 | 434.020 | 2392 |
| `rsync-1x` | 3 | 3022.208 | 3022.640 | 155.050 | 931.447 | 7852 |

#### [10x10x10x1024x1M](local/weka/10x10x10x1024x1M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-default` | 3 | 364.740 | 398.077 | 96.627 | 786.020 | 98756 |
| `rcp-32` | 3 | 392.501 | 424.476 | 94.350 | 757.493 | 98304 |
| `rcp-128` | 3 | 353.233 | 436.845 | 95.767 | 765.313 | 100072 |
| `rcp-8` | 3 | 606.819 | 648.957 | 94.467 | 700.607 | 101684 |
| `dcp-default` | 3 | 3667.075 | 3677.657 | 1812.380 | 850.537 | 70296 |
| `dcp-32` | 3 | 3848.908 | 3862.735 | 5196.713 | 808.597 | 86432 |
| `rsync-10x` | 3 | 11369.718 | 11384.909 | 208.720 | 1054.027 | 8756 |
| `cp-10x` | 3 | 11549.864 | 11587.693 | 12.007 | 540.107 | 3200 |
| `dcp-8` | 3 | 14562.347 | 14572.629 | 1127.860 | 775.463 | 206596 |

#### [10x10x10x10x1024x1K](local/weka/10x10x10x10x1024x1K.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-default` | 3 | 523.630 | 652.649 | 965.390 | 4209.060 | 1098128 |
| `rcp-128` | 3 | 584.926 | 754.368 | 943.443 | 29425.100 | 1106424 |
| `rcp-32` | 3 | 906.532 | 1080.197 | 676.517 | 1114.283 | 1093868 |
| `dcp-default` | 3 | 1765.340 | 1858.561 | 3567.177 | 1884.577 | 439044 |
| `dcp-32` | 3 | 1994.002 | 2099.496 | 4295.377 | 1747.790 | 436076 |
| `cp-10x` | 3 | 3527.927 | 3639.967 | 32.163 | 601.980 | 3132 |
| `rcp-8` | 3 | 3775.590 | 3986.129 | 689.940 | 1159.443 | 1096084 |
| `rsync-10x` | 3 | 5697.453 | 5905.308 | 105.650 | 1198.300 | 7968 |
| `dcp-8` | 3 | 7611.065 | 7809.408 | 1522.347 | 1074.330 | 1647664 |

### XFS

For the four synthetic filesets, the October 11 corrective run supplies 48 rcp rows and the September campaign supplies 96 dated cp, dcp and rsync reference rows. Each variant has three iterations. Both campaigns copied within local `/instance-store` XFS and used client `drop_caches=3`, but they ran on different nodes and dates. These are historical cross-run comparisons, not same-run tool ratios. The superseded September rcp rows and invalid October 10 run5 are excluded. Local rcp and cp can use reflink-capable copying; apparent file bytes do not establish physical storage throughput.

#### [10x100x1G](local/xfs/10x100x1G.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `cp-10x (Sep reference)` | 3 | 0.053 | 0.129 | 0.013 | 0.030 | 4380 |
| `cp-1x (Sep reference)` | 3 | 0.071 | 0.144 | 0.000 | 0.010 | 3828 |
| `rcp-8` | 3 | 0.162 | 0.213 | 0.067 | 0.080 | 10184 |
| `rcp-default` | 3 | 0.184 | 0.235 | 0.067 | 0.147 | 12516 |
| `rcp-32` | 3 | 0.189 | 0.241 | 0.077 | 0.133 | 8520 |
| `rcp-128` | 3 | 0.201 | 0.253 | 0.067 | 0.180 | 11632 |
| `dcp-32 (Sep reference)` | 3 | 302.162 | 302.226 | 0.010 | 0.020 | 9200 |
| `dcp-default (Sep reference)` | 3 | 309.596 | 309.660 | 0.010 | 0.020 | 9300 |
| `dcp-8 (Sep reference)` | 3 | 312.630 | 312.694 | 0.010 | 0.020 | 9072 |
| `dcp-128 (Sep reference)` | 3 | 314.380 | 314.448 | 0.010 | 0.020 | 10284 |
| `rsync-10x (Sep reference)` | 3 | 304.055 | 322.738 | 147.037 | 470.623 | 7964 |
| `rsync-1x (Sep reference)` | 3 | 679.865 | 684.238 | 147.033 | 389.927 | 8232 |

#### [10x1024x100M](local/xfs/10x1024x100M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `cp-10x (Sep reference)` | 3 | 0.082 | 0.174 | 0.023 | 0.270 | 4436 |
| `cp-1x (Sep reference)` | 3 | 0.276 | 0.370 | 0.010 | 0.173 | 3876 |
| `rcp-8` | 3 | 0.327 | 0.397 | 0.627 | 0.873 | 10468 |
| `rcp-32` | 3 | 0.367 | 0.437 | 0.680 | 1.437 | 10664 |
| `rcp-default` | 3 | 0.372 | 0.445 | 0.680 | 1.207 | 8500 |
| `rcp-128` | 3 | 0.382 | 0.452 | 0.617 | 1.570 | 15680 |
| `dcp-32 (Sep reference)` | 3 | 346.086 | 346.166 | 0.010 | 0.023 | 9312 |
| `dcp-default (Sep reference)` | 3 | 346.620 | 346.700 | 0.010 | 0.020 | 9456 |
| `dcp-8 (Sep reference)` | 3 | 353.083 | 353.160 | 0.010 | 0.020 | 9132 |
| `rsync-10x (Sep reference)` | 3 | 336.802 | 355.103 | 151.417 | 480.533 | 7908 |
| `dcp-128 (Sep reference)` | 3 | 370.523 | 370.597 | 0.010 | 0.023 | 10228 |
| `rsync-1x (Sep reference)` | 3 | 731.890 | 735.896 | 150.047 | 397.783 | 8972 |

#### [10x10x10x1024x1M](local/xfs/10x10x10x1024x1M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `cp-10x (Sep reference)` | 3 | 3.978 | 5.058 | 1.143 | 27.503 | 4252 |
| `rcp-8` | 3 | 20.400 | 21.599 | 62.993 | 148.117 | 89296 |
| `rcp-default` | 3 | 22.001 | 23.232 | 68.200 | 161.493 | 94944 |
| `rcp-32` | 3 | 22.274 | 23.484 | 69.720 | 162.153 | 90152 |
| `rcp-128` | 3 | 22.728 | 23.981 | 60.013 | 151.497 | 93660 |
| `cp-1x (Sep reference)` | 3 | 25.018 | 26.104 | 1.060 | 19.473 | 3900 |
| `dcp-32 (Sep reference)` | 3 | 381.995 | 382.978 | 0.010 | 0.020 | 9048 |
| `dcp-8 (Sep reference)` | 3 | 399.157 | 400.123 | 0.010 | 0.020 | 9228 |
| `dcp-default (Sep reference)` | 3 | 411.073 | 412.063 | 0.010 | 0.023 | 9264 |
| `dcp-128 (Sep reference)` | 3 | 422.296 | 423.269 | 0.013 | 0.020 | 10260 |
| `rsync-10x (Sep reference)` | 3 | 464.392 | 486.094 | 157.330 | 544.543 | 8640 |
| `rsync-1x (Sep reference)` | 3 | 903.388 | 909.726 | 154.873 | 461.610 | 9008 |

#### [10x10x10x10x1024x1K](local/xfs/10x10x10x10x1024x1K.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `cp-10x (Sep reference)` | 3 | 39.842 | 51.050 | 11.350 | 282.853 | 3780 |
| `dcp-32 (Sep reference)` | 3 | 84.537 | 94.239 | 0.010 | 0.020 | 8752 |
| `dcp-default (Sep reference)` | 3 | 85.253 | 95.026 | 0.010 | 0.020 | 8760 |
| `rsync-10x (Sep reference)` | 3 | 80.636 | 199.498 | 38.737 | 302.210 | 8264 |
| `dcp-8 (Sep reference)` | 3 | 194.737 | 204.195 | 0.010 | 0.020 | 8468 |
| `rcp-8` | 3 | 215.828 | 227.418 | 639.650 | 1566.923 | 761580 |
| `rcp-32` | 3 | 226.172 | 237.805 | 744.870 | 1787.030 | 764216 |
| `rcp-default` | 3 | 227.009 | 238.670 | 736.277 | 1783.810 | 770092 |
| `dcp-128 (Sep reference)` | 3 | 229.174 | 238.821 | 0.010 | 0.020 | 10020 |
| `rcp-128` | 3 | 230.682 | 242.351 | 655.613 | 1653.580 | 774704 |
| `cp-1x (Sep reference)` | 3 | 264.881 | 276.061 | 10.953 | 205.350 | 3268 |
| `rsync-1x (Sep reference)` | 3 | 548.709 | 566.656 | 38.620 | 261.153 | 8736 |

Run9 adds a separate Linux 7.2.9 source-tree comparison on local XFS: 94,775 regular files, 6,204 reported directories, 99 symlinks and 1,616,711,010 apparent bytes. The four tools were measured on the same source host with the same cache procedure. Rcp and cp use reflink-capable local copy settings; these times do not establish physical storage throughput.

#### [linux-source](local/xfs/linux-source.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `cp-1x` | 3 | 2.497 | 2.738 | 0.113 | 1.733 | 3092 |
| `rcp-default` | 3 | 2.484 | 2.859 | 6.457 | 20.150 | 166572 |
| `dcp-default` | 3 | 3.144 | 3.332 | 0.010 | 0.020 | 8860 |
| `rsync-1x` | 3 | 6.251 | 7.838 | 0.667 | 3.247 | 11832 |

### NFS

#### [10x100x1G](local/nfs/10x100x1G.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `dcp-8` | 3 | 940.252 | 940.332 | 0.017 | 0.020 | 8844 |
| `dcp-32` | 3 | 973.904 | 973.986 | 0.013 | 0.020 | 9252 |
| `dcp-default` | 3 | 992.468 | 992.549 | 0.013 | 0.020 | 9068 |
| `rcp-8` | 3 | 1018.585 | 1018.792 | 0.180 | 202.800 | 9804 |
| `rcp-default` | 3 | 1019.296 | 1019.511 | 0.170 | 212.167 | 12028 |
| `cp-10x` | 3 | 1023.647 | 1023.862 | 0.040 | 203.273 | 4408 |
| `rsync-10x` | 3 | 1035.371 | 1035.579 | 147.603 | 426.600 | 9368 |
| `dcp-128` | 3 | 1067.676 | 1067.756 | 0.010 | 0.020 | 10288 |
| `rcp-128` | 3 | 1072.172 | 1072.373 | 0.147 | 209.013 | 16712 |
| `rcp-32` | 3 | 1117.314 | 1117.528 | 0.197 | 207.950 | 11296 |
| `rsync-1x` | 3 | 1621.746 | 1621.953 | 147.320 | 403.697 | 9496 |
| `cp-1x` | 3 | 2021.566 | 2021.769 | 0.020 | 176.930 | 3876 |

#### [10x1024x100M](local/nfs/10x1024x100M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-default` | 3 | 811.021 | 812.944 | 1.260 | 224.973 | 9816 |
| `rcp-32` | 3 | 843.904 | 845.835 | 1.317 | 222.260 | 9504 |
| `rcp-8` | 3 | 849.022 | 850.940 | 1.047 | 212.073 | 9976 |
| `rsync-10x` | 3 | 867.180 | 869.105 | 152.473 | 447.343 | 9420 |
| `cp-10x` | 3 | 875.151 | 877.078 | 0.170 | 211.213 | 4400 |
| `rcp-128` | 3 | 880.053 | 881.963 | 1.457 | 230.560 | 13672 |
| `dcp-default` | 3 | 920.216 | 920.329 | 0.017 | 0.020 | 9264 |
| `dcp-32` | 3 | 935.166 | 935.279 | 0.010 | 0.030 | 9320 |
| `dcp-8` | 3 | 991.832 | 991.940 | 0.017 | 0.023 | 8988 |
| `dcp-128` | 3 | 1066.402 | 1066.514 | 0.013 | 0.023 | 10256 |
| `rsync-1x` | 3 | 1603.737 | 1605.645 | 149.390 | 412.343 | 9524 |
| `cp-1x` | 3 | 2334.877 | 2336.836 | 0.100 | 180.847 | 3892 |

#### [10x10x10x1024x1M](local/nfs/10x10x10x1024x1M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-128` | 3 | 812.742 | 1195.609 | 72.000 | 465.693 | 132840 |
| `rcp-default` | 3 | 910.920 | 1293.683 | 64.703 | 431.903 | 141448 |
| `rcp-32` | 3 | 915.210 | 1297.922 | 65.203 | 429.057 | 141696 |
| `dcp-128` | 3 | 1000.566 | 1387.224 | 0.017 | 0.020 | 10260 |
| `dcp-32` | 3 | 1201.788 | 1582.541 | 0.013 | 0.027 | 9296 |
| `dcp-default` | 3 | 1234.214 | 1617.866 | 0.013 | 0.023 | 9264 |
| `rcp-8` | 3 | 1744.435 | 2127.135 | 59.540 | 409.953 | 147632 |
| `cp-10x` | 3 | 1900.438 | 2285.324 | 4.203 | 291.853 | 4808 |
| `rsync-10x` | 3 | 2081.838 | 2468.109 | 167.710 | 665.547 | 9144 |
| `dcp-8` | 3 | 2339.591 | 2722.906 | 0.017 | 0.030 | 8688 |
| `rsync-1x` | 3 | 11705.585 | 12086.272 | 162.683 | 553.697 | 9532 |
| `cp-1x` | 3 | 12125.508 | 12510.311 | 3.923 | 263.610 | 3876 |

#### [10x10x10x10x1024x1K](local/nfs/10x10x10x10x1024x1K.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-128` | 3 | 1376.780 | 5128.752 | 735.883 | 2497.657 | 1131940 |
| `dcp-128` | 3 | 2314.339 | 5976.830 | 0.020 | 0.030 | 10268 |
| `rcp-default` | 3 | 3176.639 | 6925.792 | 567.037 | 1800.657 | 1156760 |
| `rcp-32` | 3 | 3180.708 | 6931.443 | 570.423 | 1829.530 | 1155044 |
| `dcp-32` | 3 | 5189.635 | 8873.998 | 0.030 | 0.037 | 8724 |
| `dcp-default` | 3 | 5160.494 | 8877.805 | 0.037 | 0.030 | 8912 |
| `rcp-8` | 3 | 8245.858 | 11993.595 | 544.790 | 1780.533 | 1161760 |
| `cp-10x` | 3 | 9254.023 | 12923.120 | 26.907 | 812.287 | 4708 |
| `rsync-10x` | 3 | 12692.928 | 16360.958 | 71.140 | 1232.630 | 9572 |
| `dcp-8` | 3 | 13191.460 | 16880.323 | 0.067 | 0.057 | 9128 |

Serial NFS `cp-1x` and `rsync-1x` each hit the 12-hour copy cap. Their lower-bound records, including CPU and logs where available, are in the JSON. There are no successful means for these variants.

### ZFS

#### [10x100x1G](local/zfs/10x100x1G.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-default` | 3 | 0.860 | 1.075 | 0.040 | 41.077 | 8284 |
| `rcp-8` | 3 | 3.097 | 3.325 | 0.050 | 23.990 | 7780 |
| `cp-10x` | 3 | 3.760 | 4.023 | 0.010 | 35.390 | 3752 |
| `rcp-128` | 3 | 10.898 | 11.126 | 0.093 | 37.993 | 7436 |
| `rcp-32` | 3 | 15.961 | 16.192 | 0.077 | 40.033 | 7436 |
| `cp-1x` | 3 | 24.312 | 24.395 | 0.000 | 24.077 | 3236 |
| `dcp-128` | 3 | 703.728 | 703.821 | 925.597 | 3452.573 | 31288 |
| `dcp-32` | 3 | 728.596 | 728.731 | 203.483 | 677.053 | 27988 |
| `rsync-10x` | 3 | 802.343 | 806.090 | 233.817 | 924.397 | 8572 |
| `dcp-8` | 3 | 838.332 | 838.484 | 28.947 | 641.157 | 26928 |
| `rsync-1x` | 3 | 1328.397 | 1329.705 | 186.203 | 696.440 | 8424 |
| `dcp-default` | 3 | 1340.403 | 1340.519 | 450.987 | 645.000 | 27948 |

#### [10x1024x100M](local/zfs/10x1024x100M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-default` | 3 | 1.116 | 1.394 | 0.443 | 48.297 | 7996 |
| `rcp-128` | 3 | 2.994 | 3.275 | 0.457 | 77.623 | 8512 |
| `rcp-32` | 3 | 3.666 | 3.976 | 0.513 | 54.183 | 8132 |
| `rcp-8` | 3 | 3.818 | 4.101 | 0.443 | 24.717 | 7632 |
| `cp-10x` | 3 | 4.182 | 4.469 | 0.047 | 35.263 | 3760 |
| `cp-1x` | 3 | 25.074 | 25.214 | 0.037 | 24.747 | 3228 |
| `dcp-128` | 3 | 731.846 | 731.901 | 79.557 | 934.013 | 32156 |
| `dcp-32` | 3 | 758.931 | 758.981 | 63.097 | 679.710 | 28536 |
| `rsync-10x` | 3 | 810.004 | 813.520 | 247.520 | 950.707 | 8544 |
| `dcp-8` | 3 | 818.460 | 818.512 | 15.977 | 648.803 | 28648 |
| `dcp-default` | 3 | 1245.871 | 1245.923 | 141.560 | 654.337 | 28568 |
| `rsync-1x` | 3 | 1330.110 | 1331.283 | 189.853 | 696.770 | 8532 |

#### [10x10x10x1024x1M](local/zfs/10x10x10x1024x1M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `cp-10x` | 3 | 57.660 | 60.053 | 2.243 | 128.787 | 3748 |
| `rcp-32` | 3 | 63.359 | 65.946 | 82.363 | 494.487 | 116160 |
| `rcp-8` | 3 | 63.730 | 66.211 | 50.307 | 261.747 | 113296 |
| `rcp-128` | 3 | 64.645 | 67.126 | 76.597 | 600.160 | 118220 |
| `rcp-default` | 3 | 65.899 | 68.502 | 78.610 | 927.123 | 116400 |
| `cp-1x` | 3 | 102.037 | 104.399 | 1.750 | 99.343 | 3220 |
| `rsync-10x` | 3 | 688.304 | 691.495 | 266.770 | 1102.277 | 9500 |
| `dcp-128` | 3 | 705.118 | 707.230 | 276.950 | 1207.757 | 47592 |
| `dcp-32` | 3 | 731.086 | 733.117 | 104.043 | 739.250 | 64900 |
| `dcp-8` | 3 | 821.228 | 823.282 | 52.257 | 573.910 | 156408 |
| `dcp-default` | 3 | 1320.470 | 1322.530 | 112.943 | 709.017 | 62800 |
| `rsync-1x` | 3 | 3654.859 | 3657.038 | 215.000 | 764.253 | 9404 |

#### [10x10x10x10x1024x1K](local/zfs/10x10x10x10x1024x1K.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `cp-10x` | 3 | 65.272 | 100.794 | 17.050 | 579.690 | 3716 |
| `dcp-32` | 3 | 86.816 | 109.847 | 450.753 | 1907.227 | 515108 |
| `rsync-10x` | 3 | 83.083 | 114.361 | 59.570 | 912.943 | 8572 |
| `dcp-8` | 3 | 137.239 | 157.357 | 153.857 | 907.727 | 1334560 |
| `dcp-128` | 3 | 142.779 | 168.138 | 1670.350 | 5269.693 | 203816 |
| `rcp-8` | 3 | 204.917 | 274.341 | 519.600 | 1970.540 | 1110160 |
| `rcp-32` | 3 | 268.094 | 336.950 | 773.897 | 8179.243 | 1126596 |
| `rcp-128` | 3 | 293.569 | 362.977 | 771.673 | 14597.457 | 1119008 |
| `dcp-default` | 3 | 342.662 | 370.942 | 392.083 | 1678.360 | 380808 |
| `rcp-default` | 3 | 303.003 | 372.777 | 767.103 | 14322.783 | 1121040 |
| `cp-1x` | 3 | 394.103 | 424.480 | 16.437 | 374.883 | 3240 |
| `rsync-1x` | 3 | 6114.039 | 6138.945 | 59.240 | 641.120 | 10184 |

## Remote

### [10x100x1G](remote/10x100x1G.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-128` | 3 | 253.992 | 275.840 | 0.060 | 0.010 | 9328 |
| `rcp-default` | 3 | 255.370 | 277.356 | 0.057 | 0.013 | 9408 |
| `rcp-32` | 3 | 255.452 | 277.464 | 0.053 | 0.017 | 9368 |
| `rsync-10x (Sep reference)` | 3 | 258.607 | 277.818 | 1961.753 | 560.743 | 16852 |
| `rcp-8` | 3 | 279.452 | 301.127 | 0.053 | 0.020 | 10888 |
| `rsync-1x (Sep reference)` | 3 | 1789.548 | 1793.585 | 2003.453 | 502.350 | 16728 |

### [10x1024x100M](remote/10x1024x100M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-128` | 3 | 261.117 | 282.874 | 0.053 | 0.013 | 9392 |
| `rcp-default` | 3 | 261.856 | 283.992 | 0.050 | 0.020 | 11024 |
| `rcp-32` | 3 | 262.094 | 284.307 | 0.057 | 0.013 | 9352 |
| `rsync-10x (Sep reference)` | 3 | 264.740 | 284.396 | 2012.683 | 572.687 | 16724 |
| `rcp-8` | 3 | 307.215 | 329.175 | 0.060 | 0.010 | 9392 |
| `rsync-1x (Sep reference)` | 3 | 1832.147 | 1836.808 | 2039.697 | 477.443 | 16600 |

### [10x10x10x1024x1M](remote/10x10x10x1024x1M.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-32` | 3 | 277.155 | 289.811 | 0.053 | 0.017 | 9396 |
| `rcp-default` | 3 | 281.060 | 290.519 | 0.050 | 0.020 | 9384 |
| `rcp-128` | 3 | 286.770 | 294.321 | 0.057 | 0.013 | 9488 |
| `rsync-10x (Sep reference)` | 3 | 277.290 | 298.797 | 2026.500 | 576.663 | 14856 |
| `rcp-8` | 3 | 420.085 | 427.544 | 0.053 | 0.017 | 9392 |
| `rsync-1x (Sep reference)` | 3 | 1937.598 | 1943.869 | 2046.920 | 500.477 | 15008 |

### [10x10x10x10x1024x1K](remote/10x10x10x10x1024x1K.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rsync-10x (Sep reference)` | 3 | 73.788 | 94.629 | 41.580 | 69.467 | 10920 |
| `rcp-8` | 3 | 291.637 | 322.529 | 0.057 | 0.010 | 9364 |
| `rcp-default` | 3 | 349.452 | 376.859 | 0.057 | 0.013 | 10776 |
| `rcp-32` | 3 | 361.446 | 388.206 | 0.057 | 0.013 | 9356 |
| `rcp-128` | 3 | 381.416 | 412.852 | 0.053 | 0.013 | 9452 |
| `rsync-1x (Sep reference)` | 3 | 527.710 | 547.001 | 41.410 | 66.630 | 10912 |

Run9 also measured physical remote copies of the Linux source tree from `compute009206` to `compute005232`. Its rcp and rsync rows share the same host pair and workload, unlike the dated September references above. The source host dropped caches before each copy; destination-host cache dropping was not documented.

### [linux-source](remote/linux-source.json)

| Variant | n | Copy mean (s) | Completion mean (s) | User CPU mean (s) | System CPU mean (s) | Peak RSS max (KiB) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `rcp-default` | 3 | 5.945 | 8.001 | 0.060 | 0.010 | 10808 |
| `rsync-1x` | 3 | 8.749 | 10.870 | 3.487 | 1.453 | 15792 |
