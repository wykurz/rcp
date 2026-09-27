# Copy benchmarks

The benchmark suite measures complete command invocations on generated filesets. Timing is
informational: a slower copy does not fail CI. Failed commands, timeouts, incorrect copies, and
broken measurement infrastructure do fail. The suite covers tiny, medium, and large regular files;
new cases and command variants belong in [benchmarks/cases.json](../benchmarks/cases.json).

## Run a benchmark

The harness requires Python 3, GNU cp for local comparisons, rsync, and release builds of the
repository tools. `just benchmark` builds the tools first; `just benchmark-run` uses prepared
binaries. The output directory must be new. Generation, verification, and cleanup are outside the
timed interval.

```bash
just benchmark --case tiny-1m --mode local --cache linux-drop-caches --repetitions 4 --output /tmp/rcp-bench-local
just benchmark-run --case tiny-1m --mode loopback --cache linux-drop-caches --repetitions 3 --output /tmp/rcp-bench-remote
just benchmark-report /tmp/rcp-bench-remote/results.json --output /tmp/rcp-bench-site
```

Open `/tmp/rcp-bench-site/index.html` in a browser. The report is self-contained; viewing it does
not require a web server or external JavaScript service.

`loopback` uses SSH to read from `localhost`, forces rcp's remote protocol with `--force-remote`,
and selects the matching `rcpd` from the build directory. `ssh localhost true` must work. The mode
exercises an encrypted remote **pull**, but both endpoints share a machine, kernel, CPU budget and
storage unless separate filesystem roots are supplied. It is not a measurement of a physical 100G
link or two independent servers.

`local` compares rcp, GNU `cp -a`, and both rsync variants without SSH. Both rcp and cp retain their
default reflink policy; a reflink-capable filesystem can make these copies much faster. The Depot
storage profile uses ext4, and sizing must be recalibrated for other storage profiles. `cp-a` is
rejected in loopback mode because cp cannot perform a remote pull. Local and loopback measurements
have separate historical series. The first version does not provision fixtures or binaries on two
independent hosts; that requires an additional endpoint adapter.

Use `--source-root` and `--destination-root` to choose existing parent directories on different
filesystems. The harness creates unique owned subdirectories and never clears the supplied parents.
By default, the observed mount source and mountpoint identify each endpoint's storage in history.
Use `--source-storage-id` and `--destination-storage-id` to declare stable storage profiles when
device names or mountpoints change between allocations. These IDs are your declaration that the
underlying storage configuration is comparable; use different IDs when it changes. Filesystem type
and semantic mount options still distinguish series. Depot sets both IDs to `depot-root` for its
ephemeral runner root filesystem, while retaining the actual mount details in each result. Use
`--bin-dir` for a prepared release build. `--baseline-bin-dir` adds an rcp baseline using its
matching daemon; both versions use the same fixture and measurement procedure.

```bash
just benchmark-run --case tiny-1m --mode loopback --repetitions 3 \
  --source-root /mnt/source --destination-root /mnt/destination \
  --cache linux-drop-caches --output /tmp/rcp-bench-million

just benchmark-run --case tiny-10k --mode loopback \
  --files-in-flight 2,4,8,32,64,128 --repetitions 9 --output /tmp/rcp-bench-concurrency
```

The concurrency sweep preserves the other rcp defaults. In the current remote implementation,
`--max-files-in-flight=128` remains limited by the default `--max-connections=100`. Raw commands and
rcp summaries retain that distinction. This example has nine variants: the three defaults plus six
rcp limits. Nine repetitions let each variant occupy every trial position once.

## Cases and variants

| Purpose     | Case          | Directory widths | Files per leaf | File size | Total files | Logical data |
| ----------- | ------------- | ---------------- | -------------- | --------- | ----------- | ------------ |
| Performance | `tiny-1m`     | `10,10,10`       | 1,024          | 1 KiB     | 1,024,000   | 1,000 MiB    |
| Performance | `medium-128k` | `10,10,1`        | 1,280          | 256 KiB   | 128,000     | 31.25 GiB    |
| Performance | `large-120`   | `10,1,1`         | 12             | 256 MiB   | 120         | 30 GiB       |
| Smoke       | `tiny-10k`    | `10,1,1`         | 1,024          | 1 KiB     | 10,240      | 10 MiB       |
| Smoke       | `medium-4k`   | `10,1,1`         | 400            | 256 KiB   | 4,000       | 1,000 MiB    |
| Smoke       | `large-100`   | `10,1,1`         | 10             | 16 MiB    | 100         | 1,600 MiB    |

All cases have ten independent top-level partitions for ten-process variants. The million-file case
has 1,111 directories including its root. Filesystem allocation and inode space exceed logical
payload size. Source fixtures are removed after their completed case is recorded, so peak payload
storage is one source and one destination, plus filesystem and build overhead.

Performance workloads target copy durations above ten seconds, preferably around one minute. Those
are sizing targets rather than timing gates: machines and tools differ, and longer filesets cannot
remove shared-runner variability. The 256 MiB-file case covers larger individual transfers. A
cold-cache sizing pass on Depot found local rcp could copy its 30 GiB in about six seconds, while cp
and rsync took roughly 38–51 seconds. This is an explicit exception to the sizing target: the runner
had about 90 GiB free after building, which limits enlargement when both source and destination must
fit. Reports flag any performance sample below ten seconds without hiding it or failing CI. Small
cases remain useful for smoke checks; pass `--purpose smoke` to keep their timings out of trend
charts and comparison ratios. Raw measurements and validation results remain available.

The default loopback comparison includes rcp with `--summary`, rsync with `-a`, and ten concurrent
rsync processes on disjoint top-level directories. Local mode adds GNU `cp -a`. The optional
`rcp-preserve` variant adds `--preserve-settings=all`. These preservation policies are deliberately
named: default rcp does less metadata preservation than the archive variants; rcp's `all` also
preserves access times. They are not claimed to have identical metadata semantics. Universal
validation checks tree structure, regular file paths, sizes and contents. It does not certify
metadata-policy equivalence.

Select a variant with repeatable `--variant` arguments. Adding another regular-file workload or flag
combination requires a manifest entry rather than a runner change:

```json
{
  "id": "medium-files",
  "directory_widths": [10, 2],
  "files_per_leaf": 64,
  "file_size_bytes": 1048576
}
```

```json
{
  "id": "rcp-32",
  "tool": "rcp",
  "args": ["--summary", "--max-files-in-flight=32"],
  "processes": 1
}
```

Pass an alternative definition with `--manifest`. Definitions are versioned and validated; unknown
fields and invalid counts fail instead of silently changing the workload. New operation types,
special-file fixtures, or two-host orchestration need a runner extension and tests. The variant ID
`rcp-baseline` is reserved for `--baseline-bin-dir`; custom manifests cannot define it.

## Measurement and cache contract

Each case is generated once with `filegen --leaf-files` and an explicit write buffer capped at the
smaller of the file size and 1 MiB. The buffer policy is part of the fixture contract revision.
Filegen currently uses random, unseeded contents: all variants in a run share identical bytes, while
independently generated runs do not. The result records a fixture digest and the generation policy.
The fixture contract has an explicit revision; change it when generation semantics change. A filegen
release version alone does not start a new performance series.

Every trial prepares a fresh destination and its cache state, then measures from launching the first
process until the last process exits. All child exit codes are checked. Full content and directory
verification follows timing. Variant order rotates between repetitions. Logs retain rcp's own
summary alongside the external wall time.

Full positional balance requires a repetition count divisible by the number of selected variants
after adding baseline or concurrency variants. Fewer repetitions are allowed for exploratory runs,
but some variants then occupy different parts of the order; temporal or cache drift can bias their
ratios. Full loopback runs balance three variants with three repetitions; local runs balance four
with four. PR smoke checks add a baseline and use four or five repetitions respectively. Choose an
appropriate count explicitly when constructing a custom comparison.

Cache modes are procedures, not assumptions:

- `source-warm` reads source data before every trial. The destination is still fresh; no claim is
  made about warming every metadata structure or executable page.
- `linux-drop-caches` syncs and requests Linux page/dentry/inode cache reclamation immediately
  before each trial. It explicitly requires permission for noninteractive privileged cache dropping,
  affects the host globally, and fails if preparation is unavailable. Loopback drops once because
  both endpoints share a kernel.
- `uncontrolled` performs no cache preparation and labels the results accordingly. It is useful for
  quick diagnostics, not interchangeable with either controlled series.

Linux cache dropping does not establish cold ZFS ARC, L2ARC, storage-controller or device caches. On
the original servers, record the exact cache procedure and relevant ZFS cache observations. See the
[Linux cache-dropping documentation](https://www.kernel.org/doc/html/latest/admin-guide/sysctl/vm.html#drop-caches).

The headline metric is command completion, not durable completion. A copy may finish while dirty
destination pages await writeback. Post-copy synchronization is outside the measured interval; do
not compare these results to a benchmark that includes flushing to stable storage.

Timeouts terminate owned local process groups, preserve diagnostics, and abort the remaining run. A
timed-out remote operation is not followed by another measurement that might overlap surviving work.
Profiling with strace or tracing belongs in a separate diagnostic run, because it changes the
measured workload.

## Results and history

Each output directory contains versioned `results.json`, a `summary.md`, and per-process logs.
Results retain the revision and dirty status, executable fingerprints and versions, exact commands,
case definitions, cache/topology/runner context, raw repetitions, validation outcomes and summaries.
The report shows wall time over measurement date, repeat spread, commit links when repository
information is available, and same-run comparisons to rsync or a supplied rcp baseline. Completed
cases remain in the chart when a later case fails or the run is interrupted. Their summaries are
checked against successful trials; unfinished cases do not produce trend points. Revision fields are
unknown when the harness runs outside a Git repository.

Revision-baseline ratios require matching recorded tools, flags, and process counts. A variant with
different metadata or concurrency settings does not receive a ratio against that baseline.

A historical series includes the workload and measurement contract, normalized flags, runner and
meaningful environment configuration. Changing the cache policy or filesystem starts a separate
series. The comparison tools' versions and SHA-256 digests also distinguish series, so a changed
reference binary does not share a trend with the old one. Scratch paths, timestamps, random fixture
bytes and the tested rcp revision do not split the series: rcp changes are precisely what the
history should expose. Keep runner labels stable and descriptive, and distinguish materially
different storage configurations with endpoint storage IDs.

History is one immutable JSON file per run on the `benchmark-history` branch. CI artifacts preserve
full logs and per-run standalone reports; the history branch provides persistence independent of
artifact expiration. Identical duplicate run IDs are harmless; conflicting duplicates fail.
Publication appends with normal Git pushes and retries concurrent updates without force-pushing.
Validated terminal failures are also recorded, so a failed measurement remains visible in history.
Failures before the harness creates a result, such as a build failure, remain in CI logs.

The history job runs after the benchmark job has ended. It passes `--finalize-interrupted` to the
publisher so a saved `running` artifact becomes a failed history record with an explicit recovery
diagnostic. Completed trials and case summaries are retained; running trials are marked failed. The
original artifact is unchanged. For manual recovery, use this flag only after confirming the
producer has stopped. Without it, publication rejects a `running` record. A hard runner shutdown can
prevent artifact upload entirely, in which case there is no result for the history job to recover.

To render downloaded history locally:

```bash
just benchmark-report /path/to/history-checkout --output /tmp/rcp-history-site
```

## CI and stability

[Depot benchmarks](../.depot/workflows/benchmarks.yml) use separate local and loopback jobs on the
fixed 32-CPU x86 runner. PRs and main pushes run the three small smoke cases with source-warm cache
preparation. They validate the harness and retain artifacts without publishing performance history.
PR smoke jobs also build the PR base and balance four loopback or five local variants across trial
positions. Superseded PR runs are cancelled.

Weekly runs and manual dispatches measure the three performance cases with Linux cache dropping
before every copy. Manual runs can select one case or all cases and can choose source-warm instead.
Loopback uses three variants and three repetitions; local uses four of each. Each mode has a
120-minute job budget; smoke jobs retain 45 minutes. Each command has a 20-minute timeout in full
runs. Fileset generation, cache preparation, verification, and cleanup add untimed overhead. No
copies overlap within a job, and each topology gets its own runner and artifact. Scheduled and
manual main-branch results are appended to history after both producer jobs end, including valid
partial results if one job fails.

```bash
just depot-benchmark
```

This runs the workflow against the current worktree and retains measurements and artifacts. Stage
ordinary untracked files first so Depot includes them. It does not run the history-publishing job.
Benchmarking is separate from `just depot-ci`; harness behavior tests are included in normal
lint/validation. For a small local harness check, explicitly select a smoke case and purpose:

```bash
just benchmark-run --case tiny-10k --mode local --purpose smoke --output /tmp/rcp-bench-smoke
```

We do not yet have evidence that this CI environment supports reliable timing gates. Fixed runner
labels do not eliminate host contention, storage variation, cache effects or infrastructure updates.
Inspect both absolute wall time and within-run ratios: a machine change may affect all tools, while
a code regression may disproportionately affect rcp. Neither ratio alone nor a small within-run
spread proves stability across runs.

First collect repeated main-branch runs, including repeats of the same commit, and inspect their
spread. A future gate should be based on demonstrated noise bounds and paired candidate/base
measurements, with a repeat policy for suspicious results. Until then, timing remains informational
and correctness/operational failures remain visible failures.

## Enable the historical site

Scheduled and manual Depot runs on main publish performance history. Push and PR smoke jobs do not
publish history. PR jobs have read-only repository permissions and never publish. The history job
needs the Depot GitHub App to permit repository `contents: write`. It then sends a
`benchmark-history-updated` repository dispatch.

The native [Pages workflow](../.github/workflows/benchmark-pages.yml) renders with code from `main`
and treats the history branch only as data. It does not run code from a PR or the history branch.
Native GitHub Actions provides the Pages deployment environment and GitHub OIDC; benchmark
computation stays on Depot.

One-time repository setup:

1. Allow the Depot GitHub App repository Contents write access for the history job.
2. In Settings → Pages, choose **GitHub Actions** as the source.
3. Set repository Actions variable `RCP_BENCHMARK_PAGES` to `true`.
4. Dispatch **Copy benchmarks** on main to verify history publication, then run **Benchmark
   history** manually if needed.

Until Pages is enabled, run artifacts and durable history remain usable. This PR prepares the
workflow; a successful remote measurement is not proof that Pages publication has occurred. Inspect
the first post-merge history job to confirm both the history push and repository dispatch succeed
with the Depot App token.
