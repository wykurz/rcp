# Copy benchmarks

For the opt-in, manually armed AWS worker and its setup checklist, see
[AWS benchmarks](../infra/aws_benchmark/README.md). Its independent scheduled shutdown must be
configured by a trusted operator. Depot benchmark execution is retired; ordinary builds and tests
remain available and do not deploy AWS resources.

For the measured comparison across shared and local filesystems, remote transfers, and a Linux
source tree, see the [performance report](reports/performance.md). Its
[original measurement packet and analysis](../analysis/2026-10-11/README.md) are preserved
separately from the benchmark harness described here.

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

Existing **Copy benchmarks** runs remain in Depot history. Their local and loopback jobs have
summaries of median wall time and repeat range. Refs containing the retirement commit no longer
define Depot measurements; older refs and reruns can retain the workflow (see
[CI and stability](#ci-and-stability)). For the full report, download the corresponding
`benchmark-results-local` or `benchmark-results-loopback` artifact, extract it, and open
`report/index.html` in a browser. The report is self-contained and needs no server. The same
artifact contains `results.json`, `summary.md`, and per-process logs; artifacts are retained for 90
days.

Historical PR and push reports list smoke runs; their timings are in `summary.md` and
`results.json`, excluded from performance trends and ratios. Earlier scheduled and manual
performance runs saved durable JSON records to the `benchmark-history` branch. Those records are
retained without starting new benchmark jobs. After
[enabling the historical site](#enable-the-historical-site), the
[Benchmark history and reports workflow](https://github.com/wykurz/rcp/actions/workflows/benchmark-pages.yml)
publishes those records as a GitHub Pages dashboard under `ci-benchmarks/`. The
[CI benchmark dashboard](https://www.wykurz.org/rcp/ci-benchmarks/) is a work in progress for
contributors; the [project homepage](https://www.wykurz.org/rcp/) and
[maintained reports](https://www.wykurz.org/rcp/reports/) are the public introduction. Until
publication is enabled, use the downloadable reports or render the history branch locally.

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

### Weekly full-preservation coverage

After a successful weekly AWS submission, a separate GitHub Actions job reuses the already-built
`rcp`, `rcpd`, and `filegen` binaries to measure `--preserve-settings=all`. It runs on
`ubuntu-24.04` with read-only repository access. It makes no AWS calls and adds no EC2 work.
Disabling or rejecting the scheduled submission also skips this job; manual submissions do not run
it.

The [preservation manifest](../benchmarks/preservation-cases.json) has 100,000 1 KiB files and
16,000 64 KiB files. Each case runs three times locally and three times as an encrypted localhost
pull, using a private SSH endpoint and the matching daemon. The source is warmed, detailed timings
are disabled, and the ordinary harness checks every payload outside the command timer. A separate
untimed one-file probe checks mode, uid, gid, and nanosecond atime/mtime before reading the
destination payload. That probe does not certify metadata on every measured file or foreign-owner
preservation.

Twelve timed copies create 696,000 destination files and transfer about 6.43 GiB in total. Commands
have a 120-second timeout and the entire job has a 15-minute limit, including fixture generation,
verification and cleanup. Actual hosted runtime still depends on the runner. This coverage reuses
the build; it does not add a Depot benchmark run.

The workflow summary shows each topology's medians and repeat ranges. Its `preservation-*` artifact
retains `results.json`, `summary.md`, and the separate `metadata.json` probe results for 90 days.
Records use the explicit `rcp-preserve` variant and `actions-ubuntu24-preserve-v1` runner label,
retaining binary hashes, revision, environment and exact commands. These shared-runner measurements
are informational and separate from the EC2 matrix and its durable history. Failures of the
commands, verification or harness fail the job; timing regressions alone do not.

To exercise the same launcher locally with prepared release binaries:

```bash
python3 -m benchmarks.weekly_preserve --bin-dir target/x86_64-unknown-linux-musl/release \
  --scratch-root /tmp/rcp-preserve-scratch --output /tmp/rcp-preserve-results
```

The output directory must be new. `--manifest` can select a smaller fixture for a launcher smoke
test while retaining the `rcp-preserve` variant. The launcher needs OpenSSH server/client tools and
an account the private unprivileged `sshd` can serve; it does not use the user's authorized keys.

### Manual measurements

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
the server and configuration for new runs; without it, that part of the transport is unknown.
Historical Depot runs supplied a digest of the actual daemon binary and the source-owned SSH setup
action, so changes to either start a new series without recording keys or temporary configuration
paths.

`local` compares rcp, GNU `cp -a`, and both rsync variants without SSH. Both rcp and cp retain their
default reflink policy; a reflink-capable filesystem can make these copies much faster. The
historical Depot storage profile used ext4, and sizing must be recalibrated for other storage
profiles. `cp-a` is rejected in loopback mode because cp cannot perform a remote pull. Local and
loopback measurements have separate historical series. The first version does not provision fixtures
or binaries on two independent hosts; that requires an additional endpoint adapter.

Use `--source-root` and `--destination-root` to choose existing parent directories on different
filesystems. The harness creates unique owned subdirectories and never clears the supplied parents.
By default, the observed mount source and mountpoint identify each endpoint's storage in history.
Use `--source-storage-id` and `--destination-storage-id` to declare stable storage profiles when
device names or mountpoints change between allocations. These IDs are your declaration that the
underlying storage configuration is comparable; use different IDs when it changes. Filesystem type
and semantic mount options still distinguish series. Historical Depot runs set both IDs to
`depot-root` for their ephemeral runner root filesystems, while retaining the actual mount details
in each result. Use `--bin-dir` for a prepared release build. `--baseline-bin-dir` adds an rcp
baseline using its matching daemon; both versions use the same fixture and measurement procedure.

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

For the separate, explicitly privileged native functional smoke with fresh per-copy cgroup
accounting, see [Literal-loopback native smoke](literal-loopback.md). It is not wired into the
benchmark schedule or historical series.

## Optional command clock observations

`--command-clocks` adds local command-boundary MONOTONIC, MONOTONIC_RAW and REALTIME observations.
It changes neither `elapsed_seconds` nor the monotonic timeout policy. Loopback and owned remote
execution are rejected before probing tools. The option is independent of pairing and resources, but
like them it records the environment allowlist; its policy and allowlisted values distinguish
historical series. Omitting it preserves the existing record shape.

Immediately before the existing start timestamp, the runner reads MONOTONIC, RAW, REALTIME and
MONOTONIC in that order. Each child waiter takes the same observation after its existing completion
timestamp. The end observation belongs to the child with the last completion timestamp, even if
another waiter's clock reads finish later. These are nearby observations, not replacement timestamps
for the exact command interval. A resource-wrapped command observes the GNU time supervisor's
completion. A launch failure without a completed child has no end observation; failures before
execution and interruptions that cannot return an outcome may lack observations.

Raw JSON retains only the boundary readings as integer nanoseconds or explicit
unsupported/read-failed states, plus the completing child's index. Reports and sanitized exports
derive signed read skew, clock deltas and differences when read. Acquisition policies version the
sampling contract; derived diagnostics can evolve without changing stored observations. Ordered
monotonic brackets bound the elapsed monotonic interval between the sequential clock reads. These
bounds describe read skew, not clock accuracy. Any reversal in the ordered MONOTONIC reads is
reported as a regression and provides no such bounds; RAW/REALTIME discontinuities remain signed.
Imports validate the primitive readings and completion evidence. Reports and sanitized exports show
qualification and numeric differences; exports omit absolute clock epochs. Clock data never accepts
or rejects a performance result automatically.

RAW is not an external elapsed-time reference. Rate differences or clock adjustments qualify the
observed durations; do not rescale results, attribute a cause from endpoints alone, or assume that
agreement at the boundaries proves nothing changed between them. Any clock reads add measurement
overhead. Keep the option identical across compared roles and retain a separate total campaign
budget; per-command timeouts do not bound fixture preparation, verification or cleanup.

## Adjacent local pairs and process resources

`--paired-seed SEED` opts into adjacent candidate/reference pairs instead of the default rotating
variant order. Select only `--variant rcp-default` and supply `--baseline-bin-dir`; both roles must
use identical single-process arguments in local mode. Paired v1 requires `--no-timings` and rejects
explicit timing arguments in the manifest so both roles use the same instrumentation.
`--repetitions` counts **pairs per case** and must be positive and even. Each two-pair block
contains one candidate-first and one reference-first pair. SHA-256 of `seed:case_id:block` chooses
the first order; the second reverses it. The seed is a JavaScript-safe integer from 0
through 9007199254740991. Results record policy `adjacent-balanced-sha256-v1`, seed, pair, block,
position (zero-based), and both role IDs. Scheduling is reproducible independently of Python's PRNG.

The roles remain adjacent in execution order, with preparation, the selected verification mode and
cleanup between commands. Full-content verification is the default; explicitly selected
metadata-size performance runs do not prove content integrity. They share one generated source but
receive fresh destinations. A failure stops the run, retains diagnostics and owned failure trees,
and never invents a missing measurement. Only pairs from fully validated cases with both committed
summaries get candidate/reference wall ratios in `summary.md` and the sanitized run envelope; raw
trial indices preserve the pairing. A partial prefix remains readable, but its unfinished case
supplies no ratios. Earlier completed cases survive a later case failure. Ratios retain their run
status and smoke/performance purpose and are not acceptance gates or statistical tests. Ordinary
historical median ratios are still separate, unpaired summaries. Use a custom manifest to fix
concurrency and runtime flags on `rcp-default`; paired mode intentionally rejects sweeps and
additional variants. Smoke-purpose ratios, if explicitly paired, are diagnostic only.

`--local-resources` independently opts into Linux GNU time measurements for one local command per
trial. The Nix development environment provides GNU time; `--resource-time PATH` can select another
GNU time executable explicitly. Its digest and version are recorded. The runner launches this small
supervisor through a fresh exec, then GNU time forks and waits for the measured command. This avoids
the large Python fixture inventory contaminating child peak RSS through a direct post-fork wait4
measurement. Synthetic tests exercise that distinction with a resident 96 MiB parent.

Metrics are user/system CPU seconds (GNU time's hundredth-second resolution), peak RSS in KiB,
voluntary/involuntary context switches, minor/major faults, and exit code. They cover the local
command's threads and waited descendants according to Linux process accounting. RSS is the kernel's
per-command maximum, not sampled simultaneous process-tree RSS or summed peaks. Remote servers and
unwaited descendants are not attributed; remote and multi-process variants are rejected. The
supervisor adds launch/wait overhead to command wall time. Raw `resources.json` bytes and their
SHA-256 are retained next to stdout/stderr for accepted measurements. Resource metrics are accepted
only after successful command execution. A failed or timed-out command retains raw logs but has
unavailable metrics: GNU time cannot distinguish failed exec from payload exits 126/127, and its
exit field alone does not identify signal termination. In this opt-in mode, `exit_codes` explicitly
have `exit_status_scope: resource-supervisor`; failed payload status is `unconfirmed`. Supervisor
launch errors and timeouts remain primary diagnostics, independent of resource-file errors. Imports
require one recorded command and explicit supervisor/payload qualification for executed resource
trials. A recorded timeout or launch error counts as an execution attempt even when exit codes are
absent. Failures before command execution is attempted may omit those qualifications. Process-group
cleanup gives descendants their grace period even after the supervisor exits. Unwrapped command
status behavior is unchanged. Missing or malformed measurements after successful execution fail the
trial. Resource statuses are `complete`, `invalid` and `unavailable`; only `complete` carries
accepted metrics. An `invalid` record requires confirmed successful execution and a SHA-256
fingerprint of the malformed resource bytes. Failed execution requires `unavailable` with reason
`execution_failed`, regardless of resource-file contents. Both supervisor and child use `LC_ALL=C`
and `LANG=C`.

The AWS retained-report site opens on the copy duration of every file set: one small plot each, in a
row for local copies and, once a run has a remote cell, a row for remote copies over loopback. Each
row leads to a page with the duration, CPU and memory plots and the data tables of that kind of
copy. The trend follows the build under test with rcp's default settings. Each point is one
qualified run and summarizes its copies: when the run's baseline and candidate binaries are
identical every copy counts, otherwise only the candidate's copies do. A point shows the median copy
wall time (a single contributing measurement keeps its frozen median), the median of user plus
system CPU seconds **computed per copy**, and the median per-copy peak RSS converted from KiB to
MiB. Whiskers are repeat minima/maxima, not confidence intervals. These are command-scope
statistics, not total host CPU or simultaneous process-tree memory; repetitions are not summed. The
x-axis is the benchmark start recorded by the worker before fixture preparation, not collection or
retirement time. The value axis is fitted to the data and is never narrower than 5% either side of
its centre.

Runs of a file set are joined in time order across code versions, harness revisions, frozen series
IDs and machine instances. A new line starts only when the measurement contract changes or a
recorded hardware value differs from the last recorded one: CPU model or count, architecture,
storage devices, layout or filesystem. Any other recorded change, such as the kernel, is marked on
the line, and a field that an older run did not record is not a change. Resource points require
complete resource metrics for every plotted copy. Missing evidence is unavailable, never zero-filled
or interpolated, and no line crosses it. Run links and the data table retain provenance (frozen
series IDs and the observed setup) without rewriting frozen JSON. Failed and evidence-unavailable
runs remain in the run list without performance points; pending runs appear under collection status.
The plots cover still-retained 90-day records, not permanent history. A bounded window shows the
newest 32 qualified runs; an oversized page shortens the window further. The omitted count stays
visible and every run stays in the list. The machine, storage, datasets and method are described
once on the test setup page, where the values the runs recorded are merged into one table; each run
page keeps its own observed setup as a collapsed appendix. The setup page also says what the storage
allows: the provider's figures for the drives, why a copy of a terabyte file set cannot go faster
than the destination array takes writes (so local and remote copies that keep up with it take about
the same time, and CPU time and memory are where a change in rcp shows first), and a table of what
the arrays and the processors did during the newest run's copies. Each figure of that table is the
median over the copies of a file set and scenario and is given only when every copy has it. It comes
from the sampled command phase alone, whose counters run from the phase's first sample to its last:
a rate or a share is taken over that time, one sampling period less than there are samples, which is
up to ten seconds less than the copy took. Local copies and remote copies over loopback are separate
scenarios of a run: each has its own measurements, frozen series and trends.

Either option records phase costs: fixture generation and initial verification on the run;
preparation (including cache preparation but excluding result persistence), verification and
destination cleanup on each trial; and total elapsed run time. These completed-phase costs are
diagnostic wall times outside command timing, not an exhaustive partition: probing, result
serialization and final scratch removal also cost time. Interrupted phases can be absent; completed
measured runs must retain all three trial costs and generation, initial verification and total run
costs. With a small numeric tolerance, total must cover the accumulated generation, initial
verification and per-trial phase costs. It is not their sum: copy commands, persistence and other
unrecorded work take additional time. Total includes all phases and must not be added to them.
`source-verified` reads and checks the source before each trial without a global sync or cache drop;
it does not guarantee a fully hot cache or durable destination data. This remains command-completion
measurement, not copy-plus-sync.

For a small mechanism calibration, point both roles at the **same prepared full release**:

```bash
just benchmark-run --case tiny-10k --variant rcp-default --mode local \
  --bin-dir /release/same --baseline-bin-dir /release/same \
  --source-root /scratch/source --destination-root /scratch/destination \
  --cache source-verified --no-timings --paired-seed 20261006 --repetitions 2 \
  --local-resources --timeout 180 --purpose smoke --output /new/paired-smoke
```

Before scaling, set a finite disk/inode budget, total runtime limit and cleanup policy. Four A/A
copies can calibrate cost and plumbing; they cannot characterize a noise distribution. Do not pool
different targets, allocators, runtime flags, filesystems or cache policies, or treat a libc/target
change as an isolated allocator experiment. These options change no scheduled workflows or gates.

`--build-provenance PATH` is available only with `--mode local`. It loads a strict JSON sidecar with
`schema_version: 1` and `builds`, mapping `rcp` and/or `rcp-baseline` to declarations. Remote and
loopback runs are rejected because the sidecar describes only the local executables and environment.
Each declaration requires `binary_sha256`, `source_revision` (full commit ID), `source_dirty`
(boolean), `patch_sha256` (SHA-256 or null; required when dirty), `cargo_lock_sha256`,
`flake_lock_sha256`, `target`, `profile`, `features` (distinct strings), `rustflags` (argument
strings), and `rustc` (version text). Record the effective build configuration, not just the working
checkout: executable digests are checked, but source/configuration claims remain caller-declared and
are not build attestations. Sidecar bytes are fingerprinted. Any of the pairing, resource,
provenance or command-clock options records an allowlist of allocator/loader/runtime environment
variables; this is not a complete environment capture. Keep raw results private if their paths,
flags or declarations contain private details. Linux environment bytes represented by Python
surrogate escapes remain lossless in ASCII-escaped JSON. The report stages encoded outputs before
replacing each file atomically; this protects existing files from validation, encoding and staging
failures, but is not a transaction across all files.

Sanitized exports retain pair/order/phase metadata, accepted numeric resource metrics, historical
series IDs and composite artifact fingerprints. Supervisor version text, build configuration text
and environment values are omitted. Environment entries reveal presence only; no per-value digest is
exported. Build configuration is not anonymized: `configuration_text_withheld` marks text omission
only. Source-result and provenance-sidecar fingerprints remain available for integrity checks and
evidence linkage, not anonymization guarantees. Build-source qualification stays explicit.

Experiment policy, resource supervisor, observed environment allowlist and declared build
configuration distinguish new historical series; seeds and repetition counts do not. Feature names
are canonicalized as a set for series identity, while `rustflags` retain their order because
argument order can change the build. Existing result schemas and identities remain unchanged when
these options are absent. Imports with any pairing, resource, build-provenance or command-clock
context require the captured environment object, including an empty object when no allowlisted
variables were present.

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
preserves access times. They are not claimed to have identical metadata semantics. Default
validation checks tree structure, regular file paths, sizes and contents; explicit `metadata-size`
mode omits payload checks and does not prove file content integrity. Neither mode certifies
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
fields and invalid counts fail instead of silently changing the workload. Special-file fixtures,
additional operation types, or two-host orchestration need a runner extension and tests. The variant
ID `rcp-baseline` is reserved for `--baseline-bin-dir`; custom manifests cannot define it.

## Bounded receiver workloads

`benchmarks/receiver-performance.json` adds six explicitly selected workloads: fresh `tiny-1m`,
`medium-128k`, `large-120`, `directory-90k`, and million-file `tiny-unchanged` and `tiny-partial`.
The directory case has 90,000 files, 90,090 directories below the root and 90,091 including it. It
offers `rcp-default` and single-process `rsync-matched` (`-rp --stats`). Select both case and
variants with this manifest.

An optional case `mode` is `fresh` (the default), `unchanged`, or `partial`. This operation is
distinct from CLI `--mode local|loopback`, which selects transport. Every trial uses a new owned
destination. Updates seed independent ordinary files with exact source contents, modes and mtimes.
Partial seeds invert every byte of selected files and age their mtimes by exactly two seconds.
Selection is fixed: floor one percent of all files, evenly distributed over lexically sorted
file-containing directories and files, with the remainder assigned to the first directories. Both
file-placement policies are supported; `files_per_directory` includes the fixture root. For the
million case this selects ten files per leaf plus one in the first 240 leaves: 10,240 files. Partial
cases require at least 100 files and files no larger than 1 KiB; there is no configurable mutation
percentage.

Update operations support only single-process `rcp --summary` and `rsync -rp --stats`. Custom
arguments, concurrency sweeps, archive/cp and multi-process update variants fail before fixture
work. Fresh cases support arbitrary variants. The planner adds `--overwrite` to rcp updates and
keeps its destination operand without a trailing slash, so it updates that root. Single-process
rsync uses source and destination trailing slashes. A supplied baseline uses its matching master and
daemon with the same operation and seed procedure.

Before timing, seeds undergo full path/type/size/content, ordinary mode, exact mtime and
independence proofs; source hardlinks, ACLs, special mode bits and foreign ownership are rejected.
Single-process supported variants require exact rcp copied/unchanged counts or rsync
transferred-file/byte counts, full final contents and modes, and unchanged source
contents/modes/mtimes. Rsync's unchanged count is inferred from the verified total and transferred
count; `-rp` does not preserve final timestamps. Exact-summary variants launch timed children with
`LC_ALL=C` and `LANG=C` in a copied environment, independent of the caller locale. The parent
environment is unchanged; other fresh variants retain their inherited locale. Results and new
operation series record this locale policy. Preparation and all proofs stay outside the command
timer. Attempts are saved before destination preparation. Failed rows and destinations remain
available, and a failure stops subsequent trials. New rows and series include operation/cache
contract revisions; legacy saved identities remain readable and unchanged.

With prepared full releases and usable localhost SSH, use a new output path for every command:

```bash
just benchmark-run --manifest benchmarks/receiver-performance.json \
  --case medium-128k --mode loopback --cache source-verified \
  --variant rcp-default --variant rsync-matched \
  --bin-dir /release/candidate --baseline-bin-dir /release/reference \
  --no-timings --repetitions 3 --output /new/medium-localhost

just benchmark-run --manifest benchmarks/receiver-performance.json \
  --case tiny-partial --mode loopback --cache source-verified \
  --variant rcp-default --variant rsync-matched \
  --bin-dir /release/candidate --baseline-bin-dir /release/reference \
  --no-timings --repetitions 3 --output /new/partial-localhost
```

Replace the explicit case with another of the six to run that operation. Three repetitions rotate
candidate, matched rsync and full baseline across positions. This evaluation uses the explicit rsync
comparison screen (`candidate / rsync <= 1.20`) separately from the baseline regression screen
(`candidate / baseline <= 1.05`); retain every raw sample and its spread. These are interpretation
rules, not CI timing gates. Never pool different operations, cache procedures or transports, or
replace slow samples.

### Owned loopback RTT

`--mode loopback --rtt-ms 0|2|10` opts into an internal rootless namespace executor for each trial.
Omitted RTT keeps ordinary localhost behavior; explicit zero still selects isolation and a separate
series. This path supports only single exact-summary rcp and matched rsync variants, including a
supplied full baseline. Custom arguments, multi-process variants, concurrency sweeps, `RSYNC_RSH`
and a custom `--ssh-transport-profile` fail before fixture/tool work.

The host needs Linux, a nonzero caller UID, permitted user/network/mount/PID namespaces, and
`unshare`, `nsenter`, `ip`, `tc`, `mount`, `setpriv`, `ssh`, `sshd`, `ssh-keygen` and `ping` on
PATH. Missing or unusable prerequisites fail without a host-network fallback. Private ephemeral SSH
credentials and strict host keys route the source through `192.0.2.2` and destination-side localhost
SSH through the client namespace. Copy processes have no capabilities and set no-new-privileges. A
private read-only passwd bind preserves the caller account fields and sets its home to the owned
trial directory. StrictModes remains enabled and checks the authorized key within that home,
including when output is under `/tmp`. Accounts must have one matching local passwd entry; malformed
fields or an inconsistent effective lookup fail setup. The host passwd file and home stay untouched.
Copy environments contain that private HOME, account USER/LOGNAME/SHELL, inherited PATH with private
SSH first, and LANG/LC_ALL=C; other variables are cleared without changing the parent environment.

The runner generates fixtures, seeds updates, rotates variants and prepares the selected cache
procedure **before** each namespace setup. Setup, preflight and executable hashing precede the
command timer. The reported sample is the inner copy command's elapsed time, including
SSH/role-wrapper startup. Postflight, PID cleanup, the actual waited launcher exit and read-only
host topology checks must all pass before destination/source proofs can admit the row. Failures
retain the attempted row, provisional evidence and destination and stop subsequent trials; command
exit zero or an inner result alone is insufficient. Setup, worker, postflight and teardown have
bounded deadlines, and cleanup failures remain diagnostics after the first failure.

Pre/postflight evidence binds exact preflight bytes, private endpoint/user/mount/PID identities, SSH
configuration, routes, requested RTT, stable drained netem queues, zero drops and positive traffic
in both directions. Fresh trials require at least their full logical payload in source-to-client
traffic; updates instead retain their exact seed/transfer proof. Actual role argv, selected
executable hashes, CPU affinity/quota and inherited FD limits are retained. Automatic rcp F comes
from destination `--resolved-automatic-files-in-flight`; E comes from its negotiated
`--max-connections`, checked against source M, and P uses the observed pending multiplier. No CPU
formula, F20/E20/P80 or FD1024 invariant is imposed. Changed observed role conditions between
repetitions fail rather than pool different conditions. Logical F and actual leaf capacity differ.

For example, after validating the prerequisites on a tiny custom fixture:

```bash
just benchmark-run --manifest benchmarks/receiver-performance.json \
  --case tiny-partial --mode loopback --rtt-ms 10 --cache source-verified \
  --variant rcp-default --variant rsync-matched \
  --bin-dir /release/candidate --baseline-bin-dir /release/reference \
  --no-timings --repetitions 3 --output /new/partial-owned-rtt10
```

The bounded evaluation cells are:

| Workload                         | Operation                       | Owned RTT   |
| -------------------------------- | ------------------------------- | ----------- |
| `medium-128k`, `large-120`       | fresh                           | 0 ms        |
| `directory-90k`                  | fresh                           | 10 ms       |
| `tiny-unchanged`, `tiny-partial` | updates                         | 10 ms       |
| `tiny-1m`                        | separately declared fresh cells | 0 and 10 ms |

RTT2 is an optional diagnostic. Series identity records the per-trial namespace lifetime and
cache-before-setup order; pool samples only when these policies match. Transport series include RTT,
SSH/environment policy and stable role capacity/resource observations, while ephemeral paths, ports,
PIDs and namespace IDs are excluded. Declare the CPU and FD conditions being compared and check the
actual role evidence on each machine.

`transport/<case>/<variant>/<iteration>/` retains private request/launcher/copy records, raw role
argv, network evidence and credentials with a restricted directory mode. These artifacts and the
HTML history contain private evidence. The [sanitized JSON export](#share-an-all-row-json-export)
uses recorded symbolic path bindings and qualified role/tool identities; opaque arguments stay
withheld.

Record the storage profile and allow space/inodes for source, an independent seed/copy, logs and
overhead. Loopback remains a shared-host, reused-filesystem, command-completion experiment, not
durable-copy or physical two-host evidence. Evaluate the 20% rsync allowance separately from the
1.05 baseline screen and retain all rows.

## Measurement and cache contract

Each case is generated once with `filegen --leaf-files` and an explicit write buffer capped at the
smaller of the file size and 1 MiB. The buffer policy is part of the fixture contract revision.
Filegen currently uses random, unseeded contents: all variants in a run share identical bytes, while
independently generated runs do not. The result records a fixture digest and the generation policy.
The fixture contract has an explicit revision; change it when generation semantics change. A filegen
release version alone does not start a new performance series.

Every trial prepares an independent destination for its operation and its cache state, then measures
from launching the first process until the last process exits. All child exit codes are checked.
Full content and directory verification follows timing by default. The explicitly selected
metadata/size performance mode below omits payload hashing and does not prove content integrity.
Before recording a completed case, the harness verifies the source against the initial snapshot
under the selected validation policy. Variant order rotates between repetitions. Logs retain rcp's
own summary alongside the external wall time.

Full positional balance requires a repetition count divisible by the number of selected variants
after adding baseline or concurrency variants. Fewer repetitions are allowed for exploratory runs,
but some variants then occupy different parts of the order; temporal or cache drift can bias their
ratios. Full loopback runs balance three variants with three repetitions; local runs balance four
with four. Historical Depot PR smoke checks added a baseline and used four or five repetitions
respectively. Choose an appropriate count explicitly when constructing a custom comparison.

Cache modes are procedures, not assumptions:

- `source-warm` first calls global `sync`, then reads source data before every trial. No claim is
  made about warming every metadata structure or executable page.
- `source-verified` checks full source content, ordinary modes, ownership and exact original mtimes
  before every trial. It neither syncs nor drops host caches. This is a verification procedure, not
  proof of cache residency; it has a separate cache contract and historical series.
- `linux-drop-caches` syncs and requests Linux page/dentry/inode cache reclamation immediately
  before each trial. It explicitly requires permission for noninteractive privileged cache dropping,
  affects the host globally, and fails if preparation is unavailable. Loopback drops once because
  both endpoints share a kernel.
- `uncontrolled` performs no cache preparation and labels the results accordingly. It is useful for
  quick diagnostics, not interchangeable with either controlled series.

For `linux-drop-caches`, an explicit `--cache-timeout` bounds the whole preparation operation,
including its first sync. When omitted, AWS workers use their exact remaining workload deadline.
Standalone runs use `max(1200 seconds, 2 × --timeout)`, preserving the legacy separate sync and
cache-command allowances instead of introducing a tighter default. The timed copy's `--timeout`
stays independent. Invalid worker deadlines fail; they never fall back to a longer local allowance.

A timeout, failed cache command, cancellation or uncertain broker reply fails the trial before a
copy starts. It does not prove that rcp hung, and there is no retry or warm-cache fallback. Raw
trial records retain bounded stdout/stderr, return code or exception type, per-command elapsed time,
configured/effective allowance and truncation flags in `cache_preparation`. These diagnostics are
private evidence, not public log output. Successful broker replies include sync and drop timings.

A client timeout cannot establish that blocking kernel sync/reclaim stopped. The AWS worker keeps
its independent allocation deadline, collection reserve and shutdown guard. AWS generation and each
copy receive the remaining workload allowance instead of a shorter per-suite timer; the same root
cgroup deadline still bounds the whole measurement. The broker independently caps a request at eight
hours and the actual remaining worker budget; this grants no additional worker lifetime. It retains
bounded trusted stage observations in `worker.json` even if the client disconnects. Rebuild and
review the harness, facade and broker together in the AMI before deployment.

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

### Bounded verification for large local fixtures

`--streaming-verification` uses a bounded descriptor-based tree walk instead of retaining a record
for every file. It accepts fresh local copies with `source-warm` or `linux-drop-caches`. Directory
entries, depth, file count and file size are bounded; symlinks, hard links, special files, ACLs and
foreign ownership are rejected. Verification remains outside copy timing. No verification runs
between cache preparation and the timed copy.

- `--verification-mode full-content` is the default. It reads every file byte for the initial
  snapshot and checks destination content and source stability after each successful copy.
- `--verification-mode metadata-size` performs no initial or per-trial payload hashing. It checks
  relative paths, entry types, counts, sizes and ordinary modes. Source checks also compare owner,
  modification/change times, device and inode identity against the snapshot. **Performance-only
  validation does not prove file content integrity: same-size corruption can pass.** Source identity
  checks improve mutation detection without being a cryptographic content proof. Tool exit status
  and timeout gates remain mandatory. Raw summaries, reports and sanitized exports disclose this
  weaker validation policy.
- `--verification-workers` sets 1–32 hashing workers (default 8). Metadata mode runs inline, with no
  per-file futures. Full-content mode runs inline with one worker, and otherwise runs files up to
  `--verification-inline-bytes` inline (default 131,072 bytes; 0 disables small-file inline
  scheduling with multiple workers). The threshold accepts 0 through 1,000,000,000 bytes; the 128
  KiB default keeps decimal 1 MB files eligible for parallel hashing. It is a tuning heuristic, not
  a demonstrated optimum. `--verification-read-bytes` controls each read buffer (default 1,048,576
  bytes, range 4,096–16,777,216), and `--verification-batch-depth` bounds pending tasks as a
  worker-count multiplier (default 2, range 1–8). These settings are explicit in new result/series
  identity; historical records retain only the fields actually recorded.
- `--check-capacity` checks available blocks and inodes on distinct source/destination filesystems
  before generation and on the destination before each copy. Defaults add 20% plus 1 GiB byte
  headroom and 20% plus 1,024 inode headroom. Local tuning uses `--capacity-byte-percent`,
  `--capacity-inode-percent`, `--capacity-byte-reserve` (bytes) and `--capacity-inode-reserve`; each
  accepts a nonnegative integer below 2^63 and requires `--check-capacity`. These estimates cannot
  guarantee fit. They never relax fixture, topology or device-safety bounds. The AWS worker keeps
  the default margins.

Explicit verification controls require `--streaming-verification`; capacity checking also requires
it. Cancellation stops queued verification and requests active reads to finish at the next read
boundary, then drains workers before closing held descriptors. Interrupted verification and other
verification errors remain failed qualification, distinct from a proven mismatch. Completed tree
proofs and command exit codes survive later source, summary or metadata failures; those outcomes are
recorded separately. SIGINT retains its interrupt exit behavior after safe cleanup. An unresponsive
kernel I/O operation can still require the outer process/cgroup timeout.

## Performance-only matrix runs

`just benchmark-matrix` (`python3 -m benchmarks.matrix`) measures a list of *cells* and nothing
else. A cell is one file set under one scenario: a copy, or another tool run on a copy. It is the
runner behind the AWS matrix suites, and it runs anywhere:

```bash
just benchmark-matrix --manifest benchmarks/matrix-cases.json \
    --cell commission-64m:local --cell commission-64m:remote \
    --bin-dir target/release --source-root /scratch/source --destination-root /scratch/destination \
    --cache uncontrolled --output /tmp/matrix-results
```

Four scenarios copy a file set, and twelve time another tool on a copy:

- **`local`** copies between two directories:
  `rcp --summary --output-format=json SOURCE DESTINATION`.
- **`nfs`** and **`memory`** make the same local copy on other storage: between two mounts that show
  the run's two roots, and between two roots in memory. See
  [Cells on other storage](#cells-on-other-storage).
- **`remote`** makes the same copy through rcp's remote path on one host:
  `rcp --summary --output-format=json --force-remote --bind-ip=127.0.0.1 --rcpd-path=BIN_DIR/rcpd 127.0.0.1:SOURCE 127.0.0.1:DESTINATION`.
  rcp starts a source and a destination `rcpd` over SSH and the daemons exchange the data over
  loopback TCP, encrypted as by default. No network hardware and no added latency are involved. See
  [Remote cells](#remote-cells) for what the runner sets up.
- **`local-rcmp`**, **`local-rlink`** and **`local-rrm`** compare, hard-link and remove the tree
  that a `local` copy has just written, and **`remote-rcmp`**, **`remote-rlink`** and
  **`remote-rrm`** do the same after a `remote` copy. `nfs` and `memory` copies have theirs too
  (`nfs-rcmp`, `memory-rrm` and so on). See [Tool cells](#tool-cells).

It differs from `benchmark-run` in what it promises:

- **One build, default settings.** Every copy runs the `rcp-default` variant from `--bin-dir`, and
  every tool cell its own fixed command from the same directory. There is no baseline build, no
  pairing and no comparison; `--repetitions` (default 3) is the number of copies, or of tool runs,
  per cell and the cell's value is their median.
- **Cells are independent.** A cell that fails, hangs or hits an error nobody foresaw records its
  own outcome and the run goes on to the next cell. The run exits zero whenever it worked through
  its list, whatever the cells' outcomes. Cells of one file set share its generated fixture and run
  in the order given.
- **Timeouts catch hangs only.** `--timeout` (default 3600 s) is one generous limit for each
  command: file-set generation, cache preparation, each copy and each removal. A slow copy is a
  result, not a failure. On a worker with a fixed allocation (`RCP_BENCH_WORKER_DEADLINE_NS`), a
  command that the end of the allocation cuts short is `interrupted` and the cells after it are
  `not_run`; neither is a failure of rcp. The time left is measured again before every command,
  including between cache preparation and the copy. Nothing is removed once the allocation has
  ended, because clearing millions of files could outlast the time left and the worker's storage
  goes away with the machine.
- **Copies are not verified.** A copy counts when the command exits zero and its own `--summary`
  reports the expected numbers of files copied and unchanged. No tree is walked or hashed, before or
  after. Correctness is covered by the test suites; this runner only measures. Records, summaries
  and public pages say so wherever they show these numbers. A tool run counts the same way, on its
  exit status and the counts of its own summary.
- **Resources are what rcp reports about itself.** Every copy is asked for its summary as JSON
  (`--output-format=json`), printed as the last line of its output. The runner takes the transfer
  counts from it, and CPU time, peak memory, context switches and page faults of the `rcp` process,
  recorded under `resources`. After a remote copy the same summary carries the figures each daemon
  sent back, recorded under `role_resources.source` and `role_resources.destination`. No wrapper
  runs a command and no CPU or memory figure is measured from outside, so none includes a child
  process: a remote copy's `rcp` figures leave out the `ssh` clients it starts. A copy whose summary
  lacks a figure still counts, with its resources marked unavailable. The same holds when a process
  reports a peak memory of zero: a figure that was not read is never recorded as zero. A copy that
  prints no JSON summary fails its cell at stage `summary`.
- **A build without `--output-format` is still timed.** Before the first copy the runner tries the
  option once (`rcp --output-format=json --version`). A build that refuses it, one from before rcp
  could print its summary as JSON, is run without the option: its transfer counts are read from the
  text summary and it has no figures. `context.summary_format` says which summary a run read (`json`
  or `text`).

Cell outcomes are `complete`, `failed`, `timed_out`, `interrupted` (cut short by the end of the
allocation or by a stop signal) and `not_run`. A failed cell is not retried, and its remaining
copies or runs are skipped. Each outcome carries the stage it happened in and a short reason that
names the log to read, for example
`copy 2 failed with exit code 1; see 0.stdout.log and 0.stderr.log in logs/bulk-1m-1024gb/local/2`.
rcp writes what went wrong to its output; only its argument parser uses the error stream.

The output directory holds `results.json` (schema `rcp-benchmark-matrix-v1`), `summary.md` and the
logs. `results.json` is rewritten after every step, so a run that is stopped keeps the cells it
finished; a save that fails, for example on a full output directory, is made up for by the next one.
Each command log is cut to its first and last half mebibyte once the command has ended, so one copy
that fails for every file cannot fill a small output directory. `benchmarks.matrix.validate` checks
a document before anything is concluded from it. `--check-capacity` needs source and destination on
different filesystems, as on the AWS worker. `--system-telemetry` samples the same bounded host and
cgroup diagnostics as `benchmark-run`, with bounds for a run of up to twelve hours where
`benchmark-run` has eight (`context.system_telemetry` names them); a command's window lasts until
the next step starts. A fixture, destination or linked tree that could not be removed is counted in
`removal_failures`.

These documents are a separate format: the dashboards and change reports described below read
`benchmark-run` results only.

### Tool cells

Twelve scenarios time the other tools on real trees without generating anything for them. A tool
cell is named after the copies it follows and its tool: `local-rcmp`, `local-rlink` and `local-rrm`
follow local copies, `remote-rcmp`, `remote-rlink` and `remote-rrm` remote ones, and the same three
follow `nfs` and `memory` copies. A tool cell makes no copies of its own. Each of its runs follows
one copy of that cell, of the same file set, so that cell has to be selected too:

```bash
just benchmark-matrix --manifest benchmarks/matrix-cases.json \
    --cell commission-10k:local --cell commission-10k:local-rcmp --cell commission-10k:local-rlink \
    --cell commission-10k:local-rrm \
    --bin-dir target/release --source-root /scratch/source --destination-root /scratch/destination \
    --cache uncontrolled --output /tmp/matrix-results
```

After each copy the tools selected for it run in this order, whatever the order of the selection:

1. **`rcmp`** compares the source with the copy by file size:
   `rcmp --summary --quiet --metadata-compare=f:size --output-format=json SOURCE COPY`. A default
   rcp copy does not keep modification times, so the default comparison would report every file. The
   run counts when rcmp exits zero, which means no difference, and reports every file and directory
   as the same.
2. **`rlink`** hard-links the copy into a new tree beside it:
   `rlink --summary --output-format=json COPY TREE`. The run counts when it reports a hard link for
   every file and a directory for every directory. The new tree is then removed, outside the
   measurement.
3. **`rrm`** removes the copy: `rrm --summary --output-format=json COPY`. The run counts when it
   reports every file and directory removed.

A tool runs the same command whichever copies it follows, on the tree where it is: a tool after a
remote copy uses plain paths, no daemon and no SSH. The two are still separate cells, each with its
own runs and outcome, so that a remote copy that fails costs the tools after it and not the ones
after the local copy. On the AWS pages their runs are plotted together.

What a tool cell's numbers mean:

- **Time and resources.** The wall time of the tool's command, measured as a copy's is, and what the
  tool reports about itself in its JSON summary, recorded under `resources`. The counts that were
  checked are recorded under `tool_summary`.
- **Cache.** No cache is dropped for a tool on the disks. It finds what the copy before it left in
  memory: usually the metadata of both trees, and as much file data as fits. There `--cache` applies
  to the copies only. A tool on a cold tree is a different measurement, which the runner makes for
  `nfs` cells only (see [Cells on other storage](#cells-on-other-storage)).
- **Order.** `follows_trial_index` names the copy a run followed. The three tools see slightly
  different trees: rcmp runs first, and rrm removes a tree whose files have been linked and unlinked
  once.

A tool that fails, hangs or reports the wrong counts ends its own cell and nothing else. The copies
go on, and so do the other tools. Its outcome names the stage (`command` or `summary`) and the log,
as
`run 1 failed with exit code 2; see 0.stdout.log and 0.stderr.log in logs/tiny-1k-10240k/local-rrm/1`.
When a copy fails, the tool cells that still waited for it take its outcome at stage `copy`. Only
the tools of the selected cells have to be in `--bin-dir`.

**Removals.** A run with an rrm cell clears every tree with that rrm, as a user of the tools would:
the tree that rlink made, a file set that is done with, a copy that no rrm cell follows, and the
scratch directories. These removals are not measured; `context.removal` says `rrm`. The run does not
depend on them succeeding. When rrm fails at one, exits with an error or does not finish within the
hang limit, `rm -rf` clears that tree and every later tree of the run, and `rrm_fallback` in the
results says why. The same happens after a measured run of rrm that failed, for the copy it left. A
build whose rrm is broken therefore costs its rrm cells, not the copies after them, and an rrm that
hangs costs the run one hang limit, not one for every tree. A run without an rrm cell has no rrm and
removes with `rm -rf` (`context.removal` says `rm -rf`).

### Cells on other storage

`local` and `remote` cells keep their trees on the run's two roots. Two more copy scenarios keep
them elsewhere, each with its tool cells:

- **`nfs`** copies between two mounts that show those same two roots: `--nfs-source-root` is another
  path to the tree under `--source-root`, and `--nfs-destination-root` to the one under
  `--destination-root`. It is meant for a network filesystem, where every operation costs a request
  to a server. Only what is measured goes through the mounts: the copy, and the operands of rcmp,
  rlink and rrm. The file set is generated through the direct path, and every removal that is not
  measured (the tree rlink made, the file set, a copy that no rrm cell follows) goes through the
  direct paths too, where it takes seconds and not the minutes a slow mount could need.
- **`memory`** copies between two roots of its own, `--memory-source-root` and
  `--memory-destination-root`, meant for two filesystems in memory. Its file set is generated there.

```bash
just benchmark-matrix --manifest benchmarks/matrix-cases.json \
    --cell commission-10k:nfs --cell commission-10k:nfs-rrm \
    --bin-dir target/release --source-root /scratch/source --destination-root /scratch/destination \
    --nfs-source-root /mnt/nfs/source --nfs-destination-root /mnt/nfs/destination \
    --cache uncontrolled --output /tmp/matrix-results
```

How these cells differ from the others:

- **Cache.** With `--cache linux-drop-caches` an `nfs` cell drops the caches before each copy and
  before each tool run, recorded with the run as it is for a copy. What a client still remembers of
  a tree decides how many requests a tool sends for it, and a client forgets within seconds or a
  minute: a tool that followed its copy at once would measure the timing of the run more than the
  tool. A `memory` cell drops no cache at all, since its files have no other home.
- **Identity.** Such a cell is identified by its own storage, named with `--nfs-source-storage-id`
  and its three siblings, as the others are by `--source-storage-id` and `--destination-storage-id`.
  A mount with another delay or another protocol version is other storage and starts another line of
  results. `context.storages` records the roots, the names and the mounts found there.
- **One home per file set.** A file set is generated once, so its cells are all `memory` or all on
  the disks (`local`, `remote` and `nfs` share a fixture). A selection that mixes the two is
  refused, and so is a cell whose roots are not given.

The runner makes no mount: it is given the paths. Setting up a server, a mount and a delay is left
to whoever starts the run.

### Remote cells

A run that selects a `remote` cell starts one private SSH endpoint (`benchmarks/private_sshd.py`)
and stops it when the run ends:

- **A private sshd.** An unprivileged `sshd` listens on `127.0.0.1` only, on a free port, and
  accepts one key made for this run with `ssh-keygen`. Passwords, forwarding and user rc files are
  off. It needs `sshd`, `ssh` and `ssh-keygen` (`--sshd`, `--ssh`, `--ssh-keygen`; by default the
  ones on `PATH`, with `sshd` also looked for in `/usr/sbin`). It serves the account that runs the
  benchmark, which cannot be root.
- **An account that logs in silently.** `sshd` runs a command through the account's login shell and
  starts it in the account's home directory. rcp reads the first line an `rcpd` prints as its
  readiness record, so a login must print nothing of its own: a shell that refuses commands, or a
  home that `sshd` cannot enter ("Could not chdir to home directory …"), breaks every remote copy.
  The endpoint checks this when it starts, with one login that must succeed and print nothing.
- **A launcher named `ssh`.** rcp runs the first `ssh` on its `PATH`. For remote copies that is a
  launcher that sends every connection to the private sshd with the private key, and keeps host key
  checking on although rcp asks for it to be off.
- **Marked sessions.** `--rcpd-path` names the daemon in `--bin-dir`; no wrapper stands between
  `sshd` and `rcpd`, only the account's login shell. sshd starts each daemon in a session of its
  own, outside the copy's process group, so a daemon that hangs would keep working during the next
  copy. The private sshd therefore marks every session it starts (`SetEnv RCP_BENCH_ENDPOINT=…`, a
  value made for the run). After a remote copy that failed or timed out, the runner ends at once any
  marked process that is still running. After one that succeeded, a daemon that is still on its way
  out gets a few seconds to exit by itself first. Whatever had to be ended is counted in
  `leftover_processes`.
- **Loopback addresses everywhere.** Both operands and `--bind-ip` name `127.0.0.1`, so rcp never
  has to detect an address. Its detection enumerates network interfaces, which a sandbox without
  netlink sockets forbids; with explicit addresses a remote cell needs IPv4 sockets only.

What a remote cell's numbers include:

- **Time.** The wall time of the `rcp` command, as for a local copy. Besides the copy it holds one
  SSH connection and four short sessions through the login shell (rcp's two checks of `rcpd` and the
  two daemons), about 0.2 to 0.4 s here. That is a constant: irrelevant for the large file sets, and
  most of the time of the small commissioning ones.
- **Environment.** The daemons run in the environment `sshd` gives a session, not the runner's.
  Variables set for the runner, such as `TOKIO_WORKER_THREADS` or `MALLOC_CONF`, reach the `rcp`
  command and local copies but not the daemons that do a remote copy's work.

Keys and configuration live in a directory of their own under the temporary directory, never under
`--output`, which the AWS worker archives. The sshd log is kept at `logs/sshd.log`. `context.remote`
records the profile (`private-sshd-ipv4-loopback-v1`) and the identities of `ssh` and `sshd`.

The endpoint is stopped and its directory removed when the run finishes, fails, or is stopped by
SIGINT or SIGTERM. A runner that is killed outright (SIGKILL, out of memory) cleans up nothing:
`sshd` then keeps listening on loopback for that run's key until it is stopped by hand, and the
directory `rcp-bench-ssh-*` stays in the temporary directory. On the AWS worker the unit's control
group and private `/tmp` contain both.

An endpoint that cannot be started fails every remote cell at stage `transport`, with the reason.
The tool cells on those copies fail with them, at stage `copy`, and the other cells are left alone.
A file set that has only such cells is not generated. An sshd that dies between copies is started
again (`endpoint_restarted` on the next copy); if that fails, the remaining remote cells fail at
`transport` as well.

`python3 -m benchmarks.private_sshd` starts the endpoint, logs in once and stops: a quick check that
a machine can serve remote cells. When it fails it prints the reason and the end of sshd's log.

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

History is one immutable JSON file per run on the `benchmark-history` branch. Historical CI
artifacts contain full logs and per-run standalone reports until their original expiry; the history
branch provides persistence independent of artifact expiration. Identical duplicate run IDs are
harmless; conflicting duplicates fail. Publication appends with normal Git pushes and retries
concurrent updates without force-pushing. Validated terminal failures are also recorded, so a failed
measurement remains visible in history. Historical failures before the harness created a result,
such as a build failure, remain only in retained CI logs. There is currently no automatic history
publisher.

For manual recovery, pass `--finalize-interrupted` to the publisher only after confirming the
producer has stopped. A saved `running` artifact becomes a failed history record with an explicit
recovery diagnostic. Completed trials and case summaries are retained; running trials are marked
failed. The original artifact is unchanged. Without the flag, publication rejects a `running`
record. A hard runner shutdown can prevent artifact upload entirely, leaving no saved result to
recover.

To render downloaded history locally:

```bash
just benchmark-report /path/to/history-checkout --output /tmp/rcp-history-site
```

## Automatic change reports

The default HTML mode of `just benchmark-report` writes a browsable `changes.html`, `changes.json`
and `changes.md` beside the dashboard. `--format sanitized-json` writes only the sanitized
`export.json`. The existing Pages publication renders the full immutable history, publishes the HTML
report and evidence JSON, and adds the Markdown report to its CI job summary. The HTML report and
Markdown summary share an HTML-table renderer; metadata stays literal inside code elements. A failed
Pages deployment still produces the job summary after successful rendering, with unlinked run IDs
when no deployment URL is available. The dashboard links to the browsable report and evidence JSON.
This consumes already collected measurements: it adds no copy runs, timing gate, new schedule, or
service. Standalone single-run artifacts contain the same report, usually with no historical
reference.

For each completed case/variant, the report selects the most recent strictly earlier observation
with the same recorded series ID, repository, case and variant. The existing series contract binds
the workload, flags, reference binaries, cache, topology, timing collection and machine context.
Smoke runs, running producers, dirty trees, unknown revisions and missing or unqualified repository
identities cannot supply comparisons or references. Repository identity must be an explicit
`owner/repo` value; unknown local origins are not pooled together. Set
`GITHUB_REPOSITORY=owner/repo` in the environment of `just benchmark-run` before collecting local
results. The publisher `--repository` argument chooses the publication destination; it does not
stamp or repair producer records. Existing records with unknown origins remain reportable but cannot
be compared.

A qualified checkout revision is a clean tree with a hexadecimal commit ID. This is the harness
checkout revision, not proof of the source revision of externally supplied `--bin-dir` binaries.
Reference cells show the recorded checkout revision and timestamp; equal revisions are labeled "same
recorded checkout revision". Inspect binary hashes before treating these as identical builds.
Completed cases from terminal failed runs remain eligible and carry their enclosing failure status.
Equal timestamps do not establish ordering; an ambiguous latest reference produces no ratio. A
changed contract starts a new series and explicitly reports no compatible reference.

The ratio is **current median / reference median**: above one is slower. The report retains both raw
repeat sets, medians, ranges, checkout revision pins, run IDs, exact original input SHA256 hashes
and zero-based trial indices into `history.json`. Source identifiers are content-addressed as
`sha256:<digest>`, independent of input filenames or enumeration order. It does not pool repeats
across runs. Short performance samples identify the current or reference side. Timing statuses
remain explicit: `coarse`, `disabled`, `unsupported`, `not_applicable`, or `missing-legacy` for old
records. Nonpositive or unrepresentable ratios are withheld. The JSON and browsable HTML retain
every run, including failures before a completed case; the Markdown summary shows the latest ten
runs to bound CI output.

The dashboard trend chart remains descriptive: it includes all performance observations, including
dirty/unknown revisions or records missing repository identity that the historical report excludes.
Its same-run speedup is reference-tool median / rcp median (above one is faster). The historical
report instead uses current / previous median (above one is slower); these answer different
questions.

These are **unpaired historical observations**, not evidence that a code change caused a regression.
Matching recorded environments cannot eliminate noisy storage or host contention. Range overlap is
not a statistical test and a narrow range does not establish between-run stability. No numeric
change changes the CI exit code; malformed evidence still fails reporting. Original immutable JSON
remains on `benchmark-history` after the 90-day raw artifact lifetime. Reports are reproducible
derivatives of the available history snapshot, regenerated on publication. Reference selection uses
producer start time, not publication order: a late-arriving record can become the reference for an
already reported run and change that derived comparison. Measurements are immutable; comparisons are
not an append-only ledger. Preserve the report snapshot if an exact previous comparison must be
retained.

Selected case composition/order and repeat counts are not part of the existing per-case series
identity. The report records case order, shows repeat counts and flags differences between a pair,
but does not claim that earlier cases have no effect on later ones. Compare matching run composition
when confirming a change; splitting series by composition requires a separate compatibility-policy
decision. Full HTML/JSON reports have the same privacy scope as existing history; use the sanitized
export when sharing private runs.

Triage a concerning observation in this order:

1. Check producer status, exact case/series, binary identities and repeat ranges. Compare rcp and
   existing rsync/cp rows in the same run to spot broad runner/storage shifts. Fix correctness or
   infrastructure failures before interpreting elapsed times.
2. Inspect the linked run records and dashboard timing tables by role. Existing scope totals include
   waits and may overlap; they are not additive wall time or CPU utilization. Older records without
   timing data remain usable numeric observations with an explicit diagnostic gap.
3. Confirm on paired candidate/base samples for that workload and resource envelope. Repeat a
   suspicious pair before escalating; do not turn an uncalibrated Depot percentage into a gate.
4. After confirmation, choose a separate, bounded diagnostic run tied to the two run IDs, revisions,
   case and series. Preserve all process roles and capture overhead; keep diagnostic times out of
   performance history. The current change report does not launch this run or collect traces.

## Coverage and next automation decisions

The retired Depot suite covered fresh tiny/medium/large copies locally and over shared-host SSH
loopback. Its PR smoke runs exercised correctness and the measurement mechanism, including an rcp
base build. Weekly performance jobs compared tools without a paired previous rcp revision. The
receiver manifest adds massive unchanged/partial updates and wide directory sets; owned RTT0/2/10
support is available but was not part of that weekly matrix. Neither loopback nor a Depot runner
demonstrates physical fast-NIC, separate-server storage behavior. Deep chains are useful mechanism
checks, not the priority for representative performance coverage.

Keep three distinct budgets as coverage grows:

| Lane                     | Purpose                                                                       | Next decision                                                                                                                                            |
| ------------------------ | ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PR correctness/mechanism | Harness unit tests with cheap fixtures; fail broken copies/evidence           | Keep behavioral checks in `benchmark-test`; choose a new smoke budget before enabling any remote measurement lane                                        |
| Paired numeric samples   | Candidate/base on the same workload with balanced order and verified outcomes | Choose a bounded subset of massive fresh, update and wide-directory/RTT workloads, a baseline pin policy and repeat budget before expanding the schedule |
| Targeted diagnostics     | Explain a confirmed change with bounded evidence                              | Define one workload/role selection, timeout, byte/event cap, 90-day raw retention and durable numeric/provenance summary before automatic dispatch       |

Dial9 belongs in the diagnostic lane once there is an automated consumer and a demonstrated question
that existing `wait_rate`/admission/worker/execute scopes cannot answer. Its released API, root
`block_on`, blocking-pool visibility and elapsed-scope correlation need a deliberate integration and
overhead test. A runtime recorder alone would not provide this workflow. Detailed scopes and poll
traces should not contaminate the paired numeric samples.

Use retained Depot measurements only as historical context. Calibrate future benchmark
infrastructure with repeated same-commit pairs before comparing performance. A dedicated pair of
fast-NIC servers would add physical-network and controlled storage coverage; evaluate that benefit
against measured variance, utilization, maintenance and quoted runner/storage/retention costs. The
Depot weekly schedule is retired; retained artifacts keep their original expiry. Rendering existing
history requires no new benchmark worker or recurring measurement job.

Producer run URLs come only from `BENCHMARK_RUN_URL`. Records collected without that environment
variable can have empty URLs. A caller can set it to a verified run URL when collecting evidence.
Run IDs, checkout commits, binary fingerprints and input hashes remain the durable evidence keys,
not a promise that expired raw logs are recoverable.

## CI and stability

Refs containing the Depot retirement commit no longer define the benchmark workflow or
`just depot-benchmark` entrypoint, including PR, push, weekly and manual triggers. Removing source
files does not disable retained workflows on older refs or historical reruns. Before treating Depot
benchmark spending as stopped, verify the live workflow registration and queued/running jobs, and
update stale branches that still contain the definition or, if supported, pause that specific
workflow in Depot. Do not pause ordinary builds or tests. Ordinary Depot builds and non-benchmark
tests remain available through `just depot-ci` and the other `depot-*` test recipes. Harness
behavior tests remain in normal lint/validation; they do not launch a paid benchmark worker.

Existing result artifacts retain their original expiry, and the `benchmark-history` branch and
report renderer remain available. Retiring Depot benchmarks did not activate an AWS replacement or
any recurring benchmark schedule. AWS runs require their separately configured admission and
shutdown controls.

For a small local harness check, explicitly select a smoke case and purpose:

```bash
just benchmark-run --case tiny-10k --mode local --purpose smoke --output /tmp/rcp-bench-smoke
```

Validate any future benchmark environment before enabling timing gates. Fixed runner labels do not
eliminate host contention, storage variation, cache effects or infrastructure updates. Inspect both
absolute wall time and within-run ratios: a machine change may affect all tools, while a code
regression may disproportionately affect rcp. Neither ratio alone nor a small within-run spread
proves stability across runs.

Once a replacement measurement lane is explicitly enabled, collect repeated main-branch runs,
including repeats of the same commit, and inspect their spread. A future gate should be based on
demonstrated noise bounds and paired candidate/base measurements, with a repeat policy for
suspicious results. Until then, timing remains informational and correctness/operational failures
remain visible failures.

## Share an all-row JSON export

```bash
just benchmark-report /private/run/results.json \
  --format sanitized-json --output /new/shareable-export
```

A result directory or history with `runs/*.json` also works. The output directory must be new and
contains only `export.json`. Keep the input records and their raw evidence privately. The export
retains every attempted trial in its original order, including failed and running trials, measured
durations, exits, counts, proof qualifications, resource observations and timing values. It
preserves original run/series IDs and exact SHA256 hashes of source result files, including
identical duplicate records with different byte formatting.

Paths, account/host names, credentials, raw errors, labels and unknown fields are withheld. Captured
operand bindings become symbolic paths; unclassified operands stay null. Missing bindings make
command reproduction incomplete. Timing identifiers and unclassified scope names become aliases.
Known version formats and binary digests remain available; an executable's build source stays
unknown unless independently recorded, rather than being inferred from the repository revision.

Evidence references are opaque IDs. Preflight hashes cover the original embedded UTF-8 bytes;
postflight hashes cover a canonical embedded record and are labelled accordingly. An unrecorded raw
log hash stays unknown. Recorded proof success in an unvalidated failed trial is explicitly
unqualified. Completed-case ratios identify both series, divide candidate elapsed time by the
matched rsync or full-baseline elapsed time, and preserve the enclosing run status. They require
matching process counts and comparison arguments; incomplete comparisons have no ratio. Numeric
ratios do not evaluate acceptance. The evaluation screens are 1.20 against matched rsync and 1.05
against the full baseline; these are comparison screens, not performance guarantees.

## Enable the historical site

The Pages site hosts the project landing page at its root and [RCP reports](reports/README.md) under
`reports/`; the performance report is at `reports/performance/`. CI benchmark history and its JSON
and change reports live under `ci-benchmarks/`. Reports are generated from the maintained Markdown
files and SVG figures in `docs/reports/`. Pushes to main rebuild the combined site through the Pages
workflow. Other repository documentation remains on GitHub.

To preview reports locally, enter `nix develop`, or install `scripts/report-site-requirements.txt`
in a Python virtual environment, then run:

```bash
just report-check
python3 -m http.server 8000 --directory target/report-site
```

Open `http://localhost:8000/` for the landing page or `http://localhost:8000/reports/performance/`
for the report. Figures link to their full-size SVGs. The build checks local references, including
sections in linked Markdown documents, and copies only the referenced SVG figures; the full
measurement packet remains available in the repository. These checks run in `just lint` and required
CI on every pull request. Generated HTML is written under the ignored `target/` directory.

The dashboard and reports publish together. If either build fails, the previously published site
remains available until a successful build replaces it.

To preview a combined site with existing history, render the dashboard first:

```bash
just benchmark-report /path/to/history --output target/report-site/ci-benchmarks --reports-url ../reports/index.html
just report-site --with-benchmark-history
```

The project-site build preserves the dashboard and its history files under `ci-benchmarks/`. Old
root-level `index.html#run-…` bookmarks forward to that dashboard; root `changes.html` redirects to
the new changes page. Copies of `history.json`, `changes.json`, and `changes.md` keep existing data
links working. These compatibility routes are generated with `--with-benchmark-history`.

The shared header and stylesheet are in `scripts/report-site/header.html` and `site.css`; the
landing page uses `landing.html`, `landing.css`, and `landing.js` in the same directory, and the AWS
benchmark pages share `aws-page.html` and `aws.css`. The landing page reads its featured timings and
fileset size from the tables in `docs/reports/performance.md` and derives both charts from one
shared scale. The build rejects missing or ambiguous measurement rows and checks README section
links in the landing page and shared header. The report content remains wider than the shared header
to accommodate tables and figures; mobile gutters match throughout.

Historical Depot performance records remain in `benchmark-history`; the retired benchmark workflow
no longer writes new records. The unused `benchmark-history-updated` Pages trigger is removed. No
Depot write permission is needed to render retained history. Repository administrators should audit
and revoke any Depot Contents-write grant previously added only for the history job, retaining the
Contents-read access needed by ordinary CI. This source change does not alter App installation
permissions; if supported App controls cannot remove that obsolete grant independently, record it as
outstanding administrator cleanup rather than claiming it has been revoked.

The native [Pages workflow](../.github/workflows/benchmark-pages.yml) publishes with code from
`main` and treats the history branch only as data. Pull requests run read-only validation;
publication does not run code from a PR or the history branch. Native GitHub Actions provides the
Pages deployment environment and GitHub OIDC. Rendering historical reports does not start benchmark
computation.

One-time repository setup:

1. Audit and revoke the obsolete Depot history-writer Contents-write grant as described above;
   preserve ordinary CI read access.
2. In Settings → Pages, choose **GitHub Actions** as the source.
3. Set repository Actions variable `RCP_BENCHMARK_PAGES` to `true`.
4. Run **Benchmark history and reports** manually to publish the existing records.

Until Pages is enabled, retained run artifacts and durable history remain usable. A successful
measurement is not proof that Pages publication has occurred; inspect the Pages deployment result.
