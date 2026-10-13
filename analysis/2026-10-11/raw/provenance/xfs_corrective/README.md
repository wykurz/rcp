# Corrective local XFS rcp run: 2026-10-11

This directory contains an isolated rcp-only rerun of the September local XFS matrix using the
2026-10-10 build at Git commit `27c089508c13cc2a87101df4798930070f9fc509`.

The matrix retains the original four filesets, `rcp-default`, `rcp-8`, `rcp-32`, and `rcp-128`,
three iterations, local XFS staging, client `drop_caches=3`, five-second settling, copy-only GNU
time measurement, post-copy sync, count/byte validation, and destination cleanup. Dcp, cp, and
rsync comparison rows are intentionally not rerun. Staging still uses rsync outside measured time,
as it did in September.

Results are first written durably under
`/d/phscratch/public/user/mateusz/rcp-perf-results-20260902/run7-20261011-xfs-rcp-corrective`
and then mirrored here for analysis and archiving. A dependency-gated Slurm finalizer audits the
48-row matrix, writes `summary.md` and `comparison.md`, and seals this directory with `SHA256SUMS`
only after the benchmark job exits successfully.
