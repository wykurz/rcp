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
Full content and directory verification follows timing. Before recording a completed case, the
harness verifies that the source tree still matches its original snapshot. Variant order rotates
between repetitions. Logs retain rcp's own summary alongside the external wall time.

Full positional balance requires a repetition count divisible by the number of selected variants
after adding baseline or concurrency variants. Fewer repetitions are allowed for exploratory runs,
but some variants then occupy different parts of the order; temporal or cache drift can bias their
ratios. Full loopback runs balance three variants with three repetitions; local runs balance four
with four. PR smoke checks add a baseline and use four or five repetitions respectively. Choose an
appropriate count explicitly when constructing a custom comparison.

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

The recurring suite currently covers fresh tiny/medium/large copies locally and over shared-host SSH
loopback. PR smoke checks exercise correctness and the measurement mechanism, including an rcp base
build. Weekly performance jobs compare tools, but do not yet build a paired previous rcp revision.
The receiver manifest adds massive unchanged/partial updates and wide directory sets; owned
RTT0/2/10 support is available but not part of the weekly matrix. Neither loopback nor a Depot
runner demonstrates physical fast-NIC, separate-server storage behavior. Deep chains are useful
mechanism checks, not the priority for representative performance coverage.

Keep three distinct budgets as coverage grows:

| Lane                     | Purpose                                                                       | Next decision                                                                                                                                            |
| ------------------------ | ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PR correctness/mechanism | Cheap fixtures and existing smoke checks; fail broken copies/evidence         | Keep behavioral checks in `benchmark-test`; review the current multi-variant smoke cost separately                                                       |
| Paired numeric samples   | Candidate/base on the same workload with balanced order and verified outcomes | Choose a bounded subset of massive fresh, update and wide-directory/RTT workloads, a baseline pin policy and repeat budget before expanding the schedule |
| Targeted diagnostics     | Explain a confirmed change with bounded evidence                              | Define one workload/role selection, timeout, byte/event cap, 90-day raw retention and durable numeric/provenance summary before automatic dispatch       |

Dial9 belongs in the diagnostic lane once there is an automated consumer and a demonstrated question
that existing `wait_rate`/admission/worker/execute scopes cannot answer. Its released API, root
`block_on`, blocking-pool visibility and elapsed-scope correlation need a deliberate integration and
overhead test. A runtime recorder alone would not provide this workflow. Detailed scopes and poll
traces should not contaminate the paired numeric samples.

Use Depot as a coarse screen while calibrating repeated same-commit pairs. A dedicated pair of
fast-NIC servers would add physical-network and controlled storage coverage; evaluate that benefit
against observed Depot variance, utilization, maintenance and quoted runner/storage/retention costs.
No dedicated runners, new permissions, credentials, paid services, or additional recurring jobs are
required for this reporting workflow. The weekly schedule and 90-day artifact retention remain in
place. Producer run URLs come only from `BENCHMARK_RUN_URL`, which the current workflows do not set;
new records as well as older ones can therefore have empty URLs. A caller can set that environment
variable to a verified run URL when collecting evidence. Run IDs, checkout commits, binary
fingerprints and input hashes remain the durable evidence keys, not a promise that expired raw logs
are recoverable.

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

## Share an all-row JSON export

```bash
just benchmark-report /private/run/results.json \
  --format sanitized-json --output /new/shareable-export
```

A result directory or history with `runs/*.json` also works. The output directory must be new and
contains only `export.json`. Keep the input records and their raw evidence privately. The export
retains every attempted trial in its original order, including failed and running trials, measured
durations, exits, counts, proof qualifications, resource observations and timing values. It
preserves original run/series IDs and exact SHA256 hashes of all input files, including identical
duplicate records with different byte formatting.

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
