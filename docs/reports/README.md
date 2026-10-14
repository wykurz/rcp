# RCP reports

Measurements and technical reports for people using and operating rcp.

## Performance across shared filesystems and everyday copies

The [performance report](performance.md) compares rcp, dcp, GNU cp, and rsync on Lustre, Weka, NFS,
XFS, and ZFS, on copies between hosts, and on a Linux source tree. It presents command wall times,
automatic settings, concurrency sweeps, filesystem cloning, and the measured limits of each
comparison. Figures are SVG and can be opened at full size.

The [complete results tables](performance-results.md) list all tested settings, wall time, user and
system CPU time, peak resident memory, observed ranges, and supplemental timings. Separate remote
rcp role tables distinguish the master CLI from its source and destination daemons. The
[JSON analysis](../../analysis/2026-10-11/analysis.json) provides full-precision measurements for
programmatic use.

The [measurement evidence](../../analysis/2026-10-11/README.md) includes all original repetitions,
commands, logs, provenance, and checksums, plus the analysis script and computed results.

## Related documentation

- [Benchmarking guide](../benchmarking.md): run the benchmark suite and view ongoing performance
  history.
- [rcp usage and tuning](../../README.md): command examples and performance controls.
- [Design and reference documentation](../../README.md#documentation): implementation and
  operational details.
