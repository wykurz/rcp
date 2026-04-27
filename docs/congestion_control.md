# Automatic Congestion Control

This document describes the opt-in adaptive load control built into rcp,
rrm, rlink, filegen, rcmp, and rcpd — what problem it solves, how it
decides how hard to push, and how the components fit together. The
scope here is conceptual; specific flag names are covered in
`--help-all` on each binary.

## Table of Contents

- [Motivation](#motivation)
- [Goals and Non-Goals](#goals-and-non-goals)
- [Architecture](#architecture)
- [Why Concurrency Is the Lever](#why-concurrency-is-the-lever)
- [The Control Signal](#the-control-signal)
- [Why Our Own Load Doesn't Skew the Baseline](#why-our-own-load-doesnt-skew-the-baseline)
- [The Control Law](#the-control-law)
- [Enforcement Model](#enforcement-model)
- [What Counts as a Metadata Op](#what-counts-as-a-metadata-op)
- [Two Controllers: One per Filesystem Side](#two-controllers-one-per-filesystem-side)
- [Interaction with Static Throttles](#interaction-with-static-throttles)
- [Remote Copy](#remote-copy)
- [Tuning and Observability](#tuning-and-observability)
- [Pluggability](#pluggability)

## Motivation

On a shared high-performance distributed filesystem (Weka, Lustre, NFS),
a single aggressive client can starve everyone else. The metadata server
is a classic choke point: a multi-million-entry `rcp` or `rrm` can fan
out faster than the metadata path can absorb, inflating everyone's stat
latency. Manual throttles (`--ops-throttle`) help but require the
operator to pick a number — too low and the job takes all day, too high
and the filesystem suffers.

The observation is that the filesystem itself tells us how hard we can
push: its response latency inflates when we approach saturation. If we
watch that signal in real time and adjust our concurrency to stay at the
onset of inflation, we stay near peak throughput without overshooting.
This is the same idea TCP Vegas uses for network congestion control,
adapted to filesystem metadata operations.

## Goals and Non-Goals

**Goals:**

- Let a single rcp-family invocation push a shared filesystem hard when
  it's idle and back off gracefully when it's busy.
- Keep the default behavior unchanged — congestion control is opt-in.
- Expose every tuning knob so the feature can be evaluated and tuned in
  the field without recompiling.
- Make the control algorithm swappable; do not bake Vegas-specific
  assumptions into the rest of the stack.

**Non-goals (for this release):**

- Data-path throughput control (MB/s caps on reads/writes). The current
  controller reasons about metadata operations only.
- Rate-based network congestion control for the remote copy protocol.
- Globally-coordinated control across multiple clients. Each process
  controls its own behavior; multiple clients on the same filesystem
  respond independently, which is sufficient for good neighbor
  semantics.

## Architecture

Three concentric layers. Each is independently testable and replaceable.

```
                        ┌───────────────────────────┐
                        │         Algorithm         │
                        │ (controller trait + impl) │
                        │   sample in → decision    │
                        └─────────▲─────────┬───────┘
                                  │         │
                          samples │         │ decisions
                                  │         ▼
                        ┌─────────┴─────────────────┐
                        │       Control Loop        │
                        │  sample routing + tick +  │
                        │      decision broadcast   │
                        └─────────▲─────────┬───────┘
                                  │         │
                          probes  │         │ limits
                                  │         ▼
                        ┌─────────┴─────────────────┐
                        │        Enforcement        │
                        │  concurrency semaphores + │
                        │     rate token buckets    │
                        └─────────▲─────────────────┘
                                  │
                                  │ acquire / release
                                  │
                        ┌─────────┴─────────────────┐
                        │       Filesystem Ops      │
                        │  getdents, stat, open,    │
                        │      read, write, …       │
                        └───────────────────────────┘
```

**Algorithm.** A pure state machine. It consumes latency samples and
emits absolute limits — concurrency, rate, or both. No I/O, no clocks
except time values passed in. This purity lets a deterministic simulator
drive any algorithm through synthetic workloads for regression testing.

**Control loop.** Async glue. Each controlled resource has its own
instance, running as a lightweight task. It drains a bounded sample
queue, calls the algorithm's tick on a configurable cadence, and
publishes each emitted decision to the enforcement layer. We run one
controller per filesystem side (source vs destination) — see
[Two Controllers: One per Filesystem Side](#two-controllers-one-per-filesystem-side).

**Enforcement.** The hot-path gates — semaphores and token buckets that
the filesystem-op callers wait on. These existed before congestion
control; the new piece is that their capacity is now a moving target.

## Why Concurrency Is the Lever

The control loop measures *latency* and modifies *concurrency*. Those
two quantities are tied together by Little's Law applied to a
closed-loop system:

```
  throughput  =  in_flight / latency
```

When the filesystem is uncongested, every op costs a roughly constant
`baseline` wall-clock time. Pushing more ops in parallel grows
throughput linearly (`in_flight / baseline`) until *something*
saturates — disk, network, metadata server. Past that knee, additional
in-flight work doesn't get serviced any faster; it queues. The
`latency` term inflates while `throughput` flattens. That's the regime
we need to detect and stay just below.

So:

- **Why concurrency, not rate?** A rate cap forces the operator to
  know the filesystem's capacity in advance. A concurrency cap lets the
  *filesystem* tell us when it's saturated (via inflated latency) and
  back off without us needing to know the underlying capacity.
- **Why measure latency, not throughput?** Throughput at saturation is
  unstable — small load variations swing it across the knee
  unpredictably. Latency moves continuously and is cheaper to read:
  every op produces one sample.

The single control knob throughout this document — **`cwnd`** (Vegas's
"congestion window") — is exactly the maximum number of in-flight
operations the controller will permit at any instant. We use `cwnd`,
"concurrency cap", and "max in-flight" interchangeably; they are the
same number, just expressed in TCP terminology vs. systems terminology.

## The Control Signal

The algorithm's job is to find a `cwnd` that saturates the bottleneck
without queueing. It reasons about two derived quantities, both
collected from the same per-op latency probes:

- **Baseline latency** — the p10 percentile of recent operation
  latencies, computed each tick over a sliding window of the last
  ~4096 samples. This is the *uncongested floor*: most samples in the
  window are at least this fast.
- **Smoothed current latency** — an EWMA of recent operation latencies.
  Smoothing absorbs single-sample noise so a brief slow op doesn't kick
  the controller off course.

Their ratio is the congestion signal:

```
  ratio  =  smoothed_current / baseline
```

In normal operation `ratio >= 1.0` (the smoothed mean of the same
window is bounded below by the lower decile, modulo sample-ordering
noise). So the regime we care about is the strip between 1.0 and
"very large":

```
       latency ratio
              │
   beta ──────┤              ┌── shrink cwnd ──┐
              │              │                 │
              │              │                 │
   alpha ─────┤   ┌── hold ──┤
              │   │
     1.0  ────┤───┘
              │     grow cwnd
              │
              └───────────────────────────────── time
```

## Why Our Own Load Doesn't Skew the Baseline

A reasonable worry when reading the previous section: if the controller
is generating the load it's measuring, every sample collected at high
`cwnd` is inflated by our own queueing. Won't the baseline itself drift
up to match — leaving us with a controller that treats the inflated
latency as the new normal and never shrinks?

Three design choices keep the baseline trustworthy:

1. **The baseline is the p10 of the recent sample window, not a strict
   minimum or an average.** Every per-op latency goes into a sliding
   window capped at ~4096 samples; on each tick the controller takes
   the value at the 10th percentile of that window as the uncongested
   floor. The p10 is robust to a single fast outlier (the floor only
   moves when the lower decile of the window moves with it), naturally
   weighted toward recent samples (older entries fall out of the
   window), and tolerant of the natural per-op variance of real
   filesystems — variance that on a Weka or Lustre mount routinely
   spans an order of magnitude even on an idle metadata path. A strict
   running minimum, in contrast, would latch onto the single fastest
   sample (which may be a kernel cache hit unrepresentative of typical
   service time) and read ordinary variance as queueing.

   What the p10 admits and rejects, concretely: a workload that
   alternates between fast and slow phases will see the p10 follow
   the fast phase as long as roughly 10% of recent samples come from
   it; if the slow phase becomes overwhelmingly dominant, the p10
   drifts up too — but only after the fast phase has actually fallen
   out of representation, not on the strength of a single anomaly.
   Conversely, one freakishly fast outlier at the top of the window
   doesn't pin the baseline; the p10 absorbs it without moving.

2. **The control law shrinks `cwnd` long before samples age out.**
   When `ratio > beta`, the very next tick (50 ms by default) shrinks
   `cwnd` by one step. Continued inflation keeps shrinking it. Once
   `cwnd` has dropped enough for the queue to drain, fresh low-latency
   samples enter the window and the p10 tracks the true uncongested
   floor. So in any healthy operating regime the baseline is
   continuously re-validated long before sample age-out matters.

3. **If the window does empty under sustained saturation, the
   smoothed estimate is reset alongside it.** The case left open is
   *persistent* overload — e.g. `min_cwnd` is configured high and the
   filesystem is genuinely overloaded for more than the
   `min_latency_max_age` window (default 10s). When every sample in
   the window has aged out, the next sample establishes a new
   (inflated) floor. To prevent that newly-bad baseline from making
   the next tick look "uncongested" and grow `cwnd` further, the EWMA
   is reset at the same instant. The first sample-bearing tick after
   a reset replays the cold-start path and never adjusts `cwnd`,
   buying a full smoothing window for the controller to reconverge
   before any growth decision.

The only state the controller carries across a window reset is `cwnd`
itself — intentionally. The next ratio is computed from a fresh
baseline and a fresh smoothed current, so "current latency is normal"
can't be inferred from a window of uniformly inflated samples.

## The Control Law

Each tick, the controller bins the current `ratio` into one of three
regions and applies a fixed step to `cwnd`:

```
       ratio range          tick action
   ────────────────────────────────────────
       ratio  <  alpha      cwnd += increase_step    (grow)
   alpha <= ratio <= beta   cwnd unchanged           (hold)
       ratio  >  beta       cwnd -= decrease_step    (shrink)
```

Defaults: `alpha = 1.30`, `beta = 2.50`, `increase_step =
decrease_step = 1`. Each step is then clamped to `[min_cwnd,
max_cwnd]`. The thresholds are deliberately loose: real metadata
syscalls on a networked filesystem routinely show ewma-to-baseline
ratios in the 1.5–2.0× band even when the filesystem is *not*
congested, simply because per-op latency variance on these mounts is
naturally large. Tighter thresholds would misread that variance as
queueing and ratchet `cwnd` toward the floor.

A few worked examples make the shape concrete. Assume a 0.5ms baseline,
default thresholds, and `cwnd = 20` going into the tick:

| Smoothed current | Ratio | Decision | New `cwnd` |
|------------------|-------|----------|------------|
| 0.50 ms          | 1.00  | grow     | 21         |
| 0.60 ms          | 1.20  | grow     | 21         |
| 0.65 ms          | 1.30  | hold     | 20 (1.30 == alpha; not below) |
| 1.00 ms          | 2.00  | hold     | 20         |
| 1.25 ms          | 2.50  | hold     | 20 (2.50 == beta; not above) |
| 1.50 ms          | 3.00  | shrink   | 19         |
| 2.50 ms          | 5.00  | shrink   | 19         |
| 0.40 ms          | 0.80  | —        | unusual (see below) |

**Two things to notice in this table.** First, the law is a
*binary trigger*, not a proportional response: a ratio of 5.0 shrinks
by exactly the same one step as a ratio of 2.51. Sustained inflation
drives faster *effective* shrinkage because the ratio stays above
`beta` over many consecutive ticks — but each individual tick still
takes one step. This is the classic Vegas shape: simpler control law,
less prone to oscillation under noisy latency than a gain-tuned PID.

Second, **a ratio below 1.0 is unusual.** In the typical case the
smoothed current is the EWMA of samples whose lower decile is the p10
baseline, so the mean is bounded below by p10 modulo sample-ordering
noise. A brief excursion under 1.0 is possible if the EWMA's
exponential weight is dominated by an unusually-fast burst that has
not yet diluted into the broader window's percentile — this is benign
and the next tick treats `ratio < alpha` as growth headroom anyway.
The window-empty path preserves the rest of the invariants: when the
window is exhausted by age-out, the smoothed current is reset
alongside it so the next tick re-establishes both from the same fresh
sample stream.

The "responsiveness" of the controller is shaped by three knobs in
combination:

- `--auto-meta-tick-interval` (how often the binning above is
  evaluated; default 50 ms);
- `--auto-meta-ewma-alpha` (how much weight a single tick's mean gives
  to the smoothed current; default 0.3);
- `--auto-meta-increase-step` / `--auto-meta-decrease-step` (the per-
  tick step magnitudes).

Aggressive tuning compresses the time it takes to track a moving knee;
conservative tuning trades off some throughput for less jitter. The
defaults are deliberately on the conservative side.

## Enforcement Model

The controller's output is a fresh value for `cwnd`. The enforcement
layer maps that directly onto a semaphore whose capacity is set to the
current `cwnd` and updated every time the controller changes its
mind — so "in-flight at the syscall layer" never exceeds whatever the
algorithm last decided.

## What Counts as a Metadata Op

We probe most per-file metadata syscalls the mutating tools issue, on
both the read side and the write side. The cost of a probe is two
`Instant::now()` calls plus a non-blocking channel send (or a no-op when
no sink is installed), so wide coverage is cheap; in exchange we get a
control signal that doesn't have visibility holes the controller can be
fooled by.

What "wide coverage" means concretely:

- **Read-side metadata:** every `tokio::fs::symlink_metadata`,
  `metadata`, `read_link`, `canonicalize`, and `File::open` along the
  source-side paths of `rcp`, `rlink`, and `rrm`. Each is one probe per
  syscall.
- **Write-side metadata:** `create_dir`, `hard_link`, `symlink`,
  `remove_file`, `remove_dir`, `set_permissions`, the `chown` and
  `utimens` syscalls inside `preserve::set_*_metadata`, and the
  `File::open` / `OpenOptions::open(create:true, …)` for new files.
  Each is one probe per syscall.
- **Not probed today — `rcmp`.** The compare tool reads source and
  destination metadata via raw `tokio::fs::symlink_metadata` (see
  `common::cmp`); none of those calls are bracketed by
  `walk::run_metadata_probed`, so `rcmp` does not currently exercise
  the metadata controllers. Adding probes there is a follow-up.
- **Not probed by design — directory iteration.** Walks (`next_entry`
  + cached `file_type`) are *not* probed. `tokio::fs::ReadDir`
  amortizes a single `getdents` syscall over many `next_entry` calls,
  so most "walk probes" don't enter the kernel at all and complete in
  tens of nanoseconds. The resulting bimodal cache-hit / real-syscall
  distribution collapses any baseline a controller could derive from
  it and pins `cwnd` at the floor. The per-file metadata syscalls that
  *follow* the walk still carry a clean signal, so we let those drive
  the controllers and skip the walk path entirely.
- **Not probed by design — data path.** The read-loop and write-loop
  inside the copy pipeline, and `tokio::fs::copy` itself. Bandwidth-
  bound, not service-time-bound; a Vegas-style controller doesn't fit.
  See [Pluggability](#pluggability) for the BBR direction.

## Two Controllers: One per Filesystem Side

Filesystems vary enormously. A copy from a saturated NFS mount to a
local SSD has two completely independent service-time profiles, and
forcing them onto a single controller pessimizes both.

Following the guideline *one controller per filesystem*, we run two
metadata controllers per process — one per [`Side`]:

|              | **Source-side**                   | **Destination-side**                  |
|--------------|-----------------------------------|---------------------------------------|
| **Metadata** | source single-stat, `read_link`, source-side `File::open` | destination create / unlink / chmod / `hard_link` / `set_permissions` |

Each controller runs the same algorithm with its own independent state
(its own p10 baseline window, EWMA, `cwnd`) and its own enforcement
semaphore. A probe site is annotated with the side
(`Metadata(Source)`, `Metadata(Destination)`); the control loop and
enforcement gate are picked accordingly.

How tools map onto this:

| Tool      | meta-src                                       | meta-dst                                       |
|-----------|------------------------------------------------|------------------------------------------------|
| `rcp`     | top-level / dereferenced stats, `read_link`, file open | `create_dir`, `symlink`, file create, `set_permissions`, `chown`, `utimens` |
| `rrm`     | top-level stat                                 | `remove_file`, `remove_dir`, pre-unlink `set_permissions` |
| `rlink`   | top-level stats                                | `hard_link`, `create_dir`, `set_permissions`   |
| `rcmp`    | (no probes today — see note above)             | (no probes today — compare-only, no writes)    |
| `filegen` | —                                              | `create_dir`, `open(create:true)`, `set_permissions` |

Single-path tools (rrm, filegen) still get both controllers. rrm
exercises meta-src + meta-dst; filegen exercises only meta-dst; the
unused controller stays idle (harmless). Carrying both uniformly
keeps the wiring identical across tools.

A note on the rate dimension: both controllers gate their own
in-flight concurrency independently, but the global ops-throttle
(rate-per-second) is shared across the process. Only one controller —
destination metadata, by convention — drives that single rate gate;
the other applies concurrency only.

In remote copy, this layout maps cleanly: `rcpd-source` exercises
meta-src (only ever reads its local source filesystem), and
`rcpd-destination` exercises meta-dst (only ever mutates its local
destination filesystem). The unused channel stays idle on each side.

At the call site, each syscall is bracketed by permit acquisition and a
*probe* that measures its wall-clock duration:

```
            ┌── acquire permit (wait if at cap) ──┐
            │                                     │
            │   ┌── probe starts ─────┐           │
            │   │                     │           │
  ──────────┤   │  getdents + stat    │           ├──────────
  rate gate │   │   (the syscalls)    │           │ release
            │   │                     │           │
            │   └── probe completes ──┘           │
            │       (sample emitted)              │
            │                                     │
            └──── release permit ─────────────────┘
```

Two important properties follow from this placement:

1. **The probe starts after the permit is held.** If the probe covered
   permit-wait time, self-inflicted queueing on our own semaphore would
   get reported back to the controller as "the filesystem got slower" —
   positive feedback that could collapse `cwnd` to the floor under
   backlog.
2. **The permit is scoped tightly around the syscall, released before
   spawning or awaiting child tasks.** If a task held the permit while
   its children blocked trying to acquire one, we'd deadlock at any tree
   depth greater than `cwnd`. Tight scoping also matches the
   controller's semantic: "in-flight operations" are syscalls, not
   whole task lifetimes.

## Interaction with Static Throttles

The adaptive and static knobs compose:

```
            rate gate     concurrency gate
              (token        (semaphore)
              bucket)
  caller ───▶ [  ] ────────▶ [    ] ────▶ syscall
                ▲                ▲
                │                │
         --ops-throttle   --auto-meta-throttle
          (static rate)   (dynamic concurrency)
```

Either can be enabled independently:

- **Neither**: default — no rate or concurrency cap beyond file-descriptor
  limits. Legacy behavior.
- **Static rate only**: a hard ceiling on operations per second, useful
  for budget-bound deployments where the limit is known in advance.
- **Adaptive concurrency only**: `cwnd` alone adapts to the filesystem's
  response; this is the recommended configuration for distributed FS
  environments.
- **Both**: static ceiling plus adaptive control below the ceiling.
  Useful when a hard cap is required by policy but the operator also
  wants to back off further under transient load.

## Remote Copy

When rcp launches rcpd over SSH, the adaptive settings flow through the
same protocol used for all other configuration:

```
  rcp master                                 rcpd (remote)
  ───────────                                ─────────────
      │                                           │
      │  SSH                                      │
      │  ────────── launch command ────────────▶ │
      │         (--auto-meta-* flags)             │
      │                                           │
      │                                           │  ┌─ local control loop
      │                                           │  │  (same algorithm,
      │                                           │  │   same tunables,
      │                                           │  │   independent state)
      │                                           │  └─
      │  TLS control channel                      │
      │  ◀═══════════════════════════════════════▶│
      │         (file transfers)                  │
```

Each rcpd runs its own independent controller — there is no
globally-shared `cwnd`. Each node responds to the local part of the
filesystem it actually observes. This keeps the protocol simple and
means that an asymmetric load pattern (e.g. source is on a saturated
cluster, destination is local) naturally gets different control
responses on each side.

## Tuning and Observability

The controller exposes every tunable as a CLI flag — initial, minimum,
and maximum cwnd, the grow/shrink thresholds, the EWMA smoothing factor,
per-tick step sizes, the baseline-age-out interval, and the control-loop
tick cadence. Defaults are conservative; aggressive-but-sensible tuning
comes from field measurements.

The control loop emits structured `tracing` events on a few channels —
each tagged with a `unit` field so the two controllers can be told
apart in mixed logs (`meta-src`, `meta-dst`):

- **`tracing::info!`** at startup, describing the effective auto-meta
  configuration.
- **`tracing::trace!`** on every tick, with the published decision
  (`max_in_flight`, `rate_per_sec`). Off by default; enable when
  reproducing controller behavior tick-by-tick.
- **`tracing::debug!`** whenever the published decision changes from
  the prior tick. This is the right level for production runs — you
  see cwnd evolve over time without drowning in unchanged-cwnd churn.
  Also fires once when the control loop exits, for clean-shutdown
  confirmation.
- **`tracing::warn!`** if the internal sample queue drops events —
  typically a signal that the tick cadence or queue capacity is
  misconfigured for the workload's rate.

To watch a run in real time, point `RUST_LOG` at the congestion crate
at `debug` (or `trace` for tick-level granularity):

```sh
RUST_LOG=congestion=debug rcp --auto-meta-throttle … src dst
```

The simulator that drives regression tests is also available for offline
experimentation: it models a fluid-bottleneck filesystem and lets an
operator run a candidate controller through a configured scenario
entirely deterministically, without touching a real filesystem. This is
how new algorithms should be evaluated before shipping.

## Pluggability

The algorithm layer is behind a single trait. Shipping today:

- **No-op**: never limits. Default when adaptive control is disabled.
- **Fixed**: emits a constant cap. Regression baseline when comparing
  adaptive algorithms; also useful as an explicit "concurrency ceiling"
  knob.
- **Vegas-style**: the adaptive controller described above.

New algorithms plug into the same enforcement machinery and the same
simulator. BBR-style estimators (tracking bottleneck bandwidth and
round-trip propagation separately) are a natural next step for the
remote copy protocol, where the network and the filesystem contribute
different latency components.
