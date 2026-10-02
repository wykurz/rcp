# Copy benchmarks

The benchmark suite measures complete command invocations on generated filesets. Timing is
informational: a slower copy does not fail CI. Failed commands, timeouts, incorrect copies, and
broken measurement infrastructure do fail. The suite covers tiny, medium, and large regular files;
new cases and command variants belong in [benchmarks/cases.json](../benchmarks/cases.json).

`deep-20x80` contains twenty chains of eighty directory levels, with one 8-byte file in every
directory, including the fixture root: 1,600 directories and 1,601 files. It exercises directory
lifetime pressure with interior file work. Case manifests select exactly one of `files_per_leaf`
(deepest directories only) or `files_per_directory` (every directory including root). Cases with
root files require whole-tree variants; partitioned variants are rejected before fixture generation.

## View results

Open a **Copy benchmarks** run from its PR check or the Depot dashboard. Each local or loopback job
has a summary of median wall time and repeat range. For the full report, download the corresponding
`benchmark-results-local` or `benchmark-results-loopback` artifact, extract it, and open
`report/index.html` in a browser. The report is self-contained and needs no server. The same
artifact contains `results.json`, `summary.md`, and per-process logs; artifacts are retained for 90
days.

PR and push reports list smoke runs; their timings are in `summary.md` and `results.json`, excluded
from performance trends and ratios. Scheduled and manual performance runs on main also save durable
JSON records to the `benchmark-history` branch. After
[enabling the historical site](#enable-the-historical-site), the
[Benchmark history workflow](https://github.com/wykurz/rcp/actions/workflows/benchmark-pages.yml)
publishes those records as a GitHub Pages dashboard. Follow its `github-pages` deployment URL, also
shown in the repository's Settings → Pages, to open the site. Until publication is enabled, use the
downloadable reports or render the history branch locally.

The historical dashboard plots median copy wall time over date, with minimum-to-maximum repeat
ranges. Filter by case, variant, runner/topology/cache group, and series. Hover over a point for run
details, or expand a row for tool versions, environment, and diagnostics. Exact commands and raw
repetitions are in `results.json`. Different measurement configurations appear as separate series,
and completed cases remain visible if a later case fails.

New reports also show scoped timing tables per case, variant, repeat, and process role. The stage
columns include invocation count, finished and interrupted counts, cumulative elapsed seconds, mean,
p50, p95, and maximum. A finished scope means its logical end was reached; it does not promise that
the copy succeeded. Cumulative elapsed time sums all invocations of a scope. Different scopes and
processes can overlap, so their totals cannot be added to reconstruct command wall time. Historical
results made before scoped timing collection remain readable and are labeled as having no timings.

## Run a benchmark

The harness requires Python 3, GNU cp for local comparisons, rsync, and release builds of the
repository tools. `just benchmark` builds the tools first; `just benchmark-run` uses prepared
binaries. The output directory must be new. Generation, verification, and cleanup are outside the
timed interval.

The harness probes `rcp --help` before measurement for each selected candidate and baseline. A
binary advertising `--timings` collects coarse per-process JSON by default; an older baseline that
does not advertise it is recorded as unsupported. `--no-timings` disables collection for overhead
diagnostics. Non-rcp variants are marked not applicable. The effective policy and detected
capability are part of the historical series identity, so runs made with different timing policies
do not share a trend. Each trial has its own timing prefix under the output directory, and its
`*.timings.json` reports are embedded in `results.json`. A supported successful trial with missing
or malformed reports fails; partial reports from failed trials remain available. The prefix is an
execution artifact and is not added to the manifest variant arguments.

For deeper investigation, run `rcp` directly with `--timings-detail` alongside `--timings=PREFIX` to
include file, metadata, and tracker scopes, or with `--chrome-trace=PREFIX` to produce detailed
trace output. These diagnostic runs add overhead and should be kept separate from comparable
benchmark series.

```bash
just benchmark --case tiny-1m --mode local --cache linux-drop-caches --repetitions 4 --output /tmp/rcp-bench-local
just benchmark-run --case tiny-1m --mode loopback --cache linux-drop-caches --repetitions 3 --output /tmp/rcp-bench-remote
just benchmark-report /tmp/rcp-bench-remote/results.json --output /tmp/rcp-bench-site
```

For directory-lifetime comparisons, use `--case deep-20x80 --variant rcp-default --variant rsync-a`
with a matching `--baseline-bin-dir`. Measure both zero added RTT and a recorded nonzero RTT in an
isolated test network; ordinary loopback adds no delay. Keep fixture placement, file concurrency,
descriptor limits, timing policy, and transport identical across variants. Interior files and
leaf-only trees are different workloads and must not share a comparison.

Open `/tmp/rcp-bench-site/index.html` in a browser. The report is self-contained; viewing it does
not require a web server or external JavaScript service.

`loopback` uses SSH to read from `localhost`, forces rcp's remote protocol with `--force-remote`,
and selects the matching `rcpd` from the build directory. It also pins the remote rsync server to
the recorded local rsync executable; custom `--rsync-path` overrides are rejected.
`ssh localhost true` must work. The mode exercises an encrypted remote **pull**, but both endpoints
share a machine, kernel, CPU budget and storage unless separate filesystem roots are supplied. It is
not a measurement of a physical 100G link or two independent servers.

Loopback records the SSH client version and binary digest. Use `--ssh-transport-profile` to identify
the server and configuration when running outside CI; without it, that part of the transport is
unknown. Depot supplies a digest of the actual daemon binary and the source-owned SSH setup action,
so changes to either start a new series without recording keys or temporary configuration paths.

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

The concurrency sweep requires a selected rcp variant and preserves its other defaults. In the
current remote implementation, `--max-files-in-flight=128` remains limited by the default
`--max-connections=100`. Raw commands and rcp summaries retain that distinction. This example has
nine variants: the three defaults plus six rcp limits. Nine repetitions let each variant occupy
every trial position once.

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
verification follows timing. Before recording a completed case, the harness verifies that the source
tree still matches its original snapshot. Variant order rotates between repetitions. Logs retain
rcp's own summary alongside the external wall time.

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
series. Versions and SHA-256 digests for cp, rsync, loopback SSH, and any supplied baseline rcp/rcpd
binaries also distinguish series, as does a supplied SSH transport profile, so changed reference or
transport binaries do not share a trend with the old ones. Scratch paths, timestamps, random fixture
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

To start a performance run on a pushed revision, use **Run a workflow** in the Depot CI dashboard,
select this repository, the branch, and **Copy benchmarks**, then choose the case and cache inputs.
The equivalent CLI command for all cases on main is:

```bash
depot ci dispatch --repo wykurz/rcp --workflow benchmarks.yml --ref main \
  --input case=all --input cache=linux-drop-caches
```

Use `tiny-1m`, `medium-128k`, or `large-120` instead of `all` to select one case. A dispatch on a
branch other than main retains artifacts without publishing history. See Depot's
[manual workflow instructions](https://depot.dev/docs/ci/how-to-guides/manage-workflow-runs#manually-trigger-workflows).

To run the benchmark job against local changes without publishing history:

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
