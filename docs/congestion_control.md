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
- [Why Our Own Load Doesn't Skew the Signal](#why-our-own-load-doesnt-skew-the-signal)
- [The Control Law](#the-control-law)
- [Enforcement Model](#enforcement-model)
- [What Counts as a Metadata Op](#what-counts-as-a-metadata-op)
- [One Controller per (Side, Syscall)](#one-controller-per-side-syscall)
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
The design draws on a lineage of latency-based congestion control
from networking — TCP Vegas (Brakmo & Peterson, 1995), CoDel (Nichols
& Jacobson, 2012), and BBR (Cardwell et al, 2017) all watch some
form of latency-vs-baseline signal and adjust an outbound rate or
window in response. The controller here adapts that idea to
filesystem metadata operations, with two notable differences from
the classical Vegas shape covered in the [The Control Signal][cs]
section: it summarizes each window with a configurable percentile
rather than min/mean, and it allows the baseline and current
percentiles to differ to encode the inter-quantile spread directly.

[cs]: #the-control-signal

## Goals and Non-Goals

**Goals:**

- Let a single rcp-family invocation push a shared filesystem hard when
  it's idle and back off gracefully when it's busy.
- Keep the default behavior unchanged — congestion control is opt-in.
- Expose every tuning knob so the feature can be evaluated and tuned in
  the field without recompiling.
- Make the control algorithm swappable; do not bake controller-
  specific assumptions into the rest of the stack.

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
controller per `(filesystem-side, metadata-syscall)` pair — see
[One Controller per (Side, Syscall)](#one-controller-per-side-syscall).

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

The single control knob throughout this document — **`cwnd`** (the
TCP "congestion window") — is exactly the maximum number of in-flight
operations the controller will permit at any instant. We use `cwnd`,
"concurrency cap", and "max in-flight" interchangeably; they are the
same number, just expressed in TCP terminology vs. systems terminology.

## The Control Signal

The algorithm's job is to find a `cwnd` that saturates the bottleneck
without queueing. It reasons about two derived quantities computed
from the same per-op latency probes — a *baseline* percentile over a
long time window and a *current* percentile over a short subset:

- **Baseline latency** — `baseline_percentile` (default p10) over a
  long-horizon sample window (default 10s). The long-memory reference.
- **Current latency** — `current_percentile` (default p50) over a
  short-horizon subset (default 1s) of the same buffer. The recent
  estimate.

The two percentiles are independent knobs. Their ratio is the
congestion signal:

```
  ratio  =  current / baseline
```

There are two natural operating modes, depending on whether the
percentiles are equal or staggered. The shipped defaults stagger them
(baseline at p10, current at p50) — see the cross-percentile section
below for why.

### Matched percentiles (`baseline == current`)

Both windows summarize the same statistic (e.g. p50). When
the offered load is steady the two windows estimate the same
population statistic and the ratio stays near 1.0 *regardless* of
how heavy-tailed the per-op latency distribution is (with finite
windows the ratio fluctuates around 1.0 within sampling noise; the
larger the window relative to per-op variance, the tighter the
fluctuation). Sustained deviations away from 1.0 reflect a shift
in the recent distribution relative to history — which is what we
want to act on.

Strength: distribution shape cancels, so default `alpha` / `beta` are
universal — they don't need to be retuned per filesystem or per
syscall. Weakness: once both windows have equilibrated to a heavily-
loaded distribution, the ratio is back at 1.0 and the controller has
no signal that it's currently overloading the filesystem — it can
only see *changes* in load, not steady-state queueing.

### Cross percentiles (`baseline < current`)

Default. The shipped defaults stagger the two percentiles (baseline
at p10, current at p50). At steady state both windows still estimate
the same population, but the ratio now compares two different points
on that distribution — the inter-quantile spread. Queueing fattens
the upper tail asymmetrically, so spread grows with offered load even
at steady state. The ratio rides above 1.0 by an amount that tracks
the level of congestion, not just changes in it.

Strength: preserves a signal at steady-state heavy load that matched
mode loses. Weakness: the steady-state ratio depends on the specific
syscall's latency distribution shape, so `alpha` / `beta` are placed
relative to that shape rather than around 1.0 — the shipped defaults
(`alpha = 1.3`, `beta = 1.8`) bracket the typical p10/p50 spread of
metadata syscalls, but unusually skewed distributions may want
per-filesystem or per-syscall tuning.

### The hold band

Either way, the control law treats the strip around the steady-state
ratio as the hold band:

```
       latency ratio
              │
   beta ──────┤                       ┌── shrink cwnd ──┐
              │                       │                 │
              │                       │                 │
   alpha ─────┤    ┌─── hold ─────────┤
              │    │
              │    │   grow cwnd
              │
              └───────────────────────────────── time
```

For cross percentiles (the default p10/p50) both `alpha` and `beta`
typically sit above 1.0, set by the natural inter-quantile spread of
the distribution. For matched percentiles `alpha` typically straddles
1.0 instead.

## Why Our Own Load Doesn't Skew the Signal

A reasonable worry: if the controller is generating the load it's
measuring, every sample collected at high `cwnd` is inflated by our
own queueing. Won't the baseline itself drift up to match — leaving us
with a controller that treats the inflated latency as the new normal
and never shrinks?

The two-window design addresses this from two complementary angles
depending on which mode is active:

1. **In cross mode (the default), the inter-quantile spread tracks
   queueing directly.** Setting baseline at a lower percentile and
   current at a higher one (the shipped defaults are p10 / p50)
   makes the ratio a measure of the spread of the recent
   distribution. Saturation broadens the upper tail more than the
   lower decile — the lower decile sits near the bare service time
   even at high `cwnd` because the fastest paths through the
   metadata server still hit cache, while the upper tail grows with
   queue depth. So the spread, and therefore the ratio, rises with
   offered load and stays elevated until `cwnd` is pulled back enough
   to drain the queue. This is the key advantage over a baseline-
   vs-mean shape (the prior p10 / EWMA design): mean and p10 of a
   heavy-tailed distribution differ by a constant factor that is
   *purely a property of the distribution shape*, not of load —
   wasting threshold headroom on shape rather than spending it on
   the queueing signal. A p10-vs-p50 ratio carries the queueing
   signal directly because both points move with offered load but
   the upper one moves faster.

2. **In matched mode, the distribution shape cancels.** Each tick the
   controller takes the configured percentile of the long window
   (10s) as the baseline and the same percentile of the short window
   (1s) as the current estimate. When the per-op latency distribution
   is stationary, both estimates converge on the same population
   value and the ratio stays near 1.0 (finite-window noise gives
   small per-tick fluctuations). The natural per-op
   variance of real filesystems — variance that routinely spans an
   order of magnitude on a Weka or Lustre mount even on an idle
   metadata path — cancels out because it's present in both windows
   in the same proportion. The trade-off relative to cross mode is
   that matched mode loses the level signal once both windows have
   equilibrated to a saturated distribution: the ratio is back at
   1.0 and the controller can only see *changes* in load, not
   sustained queueing.

3. **The control law shrinks `cwnd` as soon as the recent distribution
   shifts.** When the offered load increases, the short window
   captures the shift before the long window does. Even if both
   eventually equilibrate, the transient `ratio > beta` shrinks `cwnd`
   on the very next tick. Continued inflation keeps shrinking. Once
   `cwnd` has dropped enough for the queue to drain, fresh low-
   latency samples enter the short window and the ratio drops back —
   the next tick can grow again.

4. **The window is bounded.** Samples older than `long_window` (default
   10s) are evicted on every tick, so a one-time spike doesn't pin
   the baseline forever. If the long window empties entirely under
   sustained saturation, both the baseline and the current statistic
   reset to `None` and the controller holds `cwnd` until fresh samples
   arrive — preventing a window of uniformly inflated samples from
   reading as "uncongested" via cancellation.

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

Defaults: `alpha = 1.30`, `beta = 1.80`, `increase_step =
decrease_step = 1`, with the percentiles staggered at 0.1 / 0.5
(cross p10/p50). Each step is clamped to `[min_cwnd, max_cwnd]`.

The only hard invariant on the thresholds is `0 < alpha < beta`. The
*natural* placement of `alpha` and `beta` relative to 1.0 depends on
the percentile pair:

- **Matched percentiles** produce a steady-state ratio near 1.0
  (modulo finite-window noise). With `alpha > 1.0` the controller
  is in *active* mode — at steady state it sits in the grow region,
  climbing until queueing pushes the ratio past `beta`. With
  `alpha < 1.0 < beta` it is in *passive* mode — at steady state
  it holds, growing only when the recent distribution is
  meaningfully *faster* than baseline (which happens during
  transient improvements, e.g. when other clients drop off).
- **Cross percentiles** produce a steady-state ratio above 1.0 set
  by the inter-quantile spread of the per-syscall latency
  distribution. Both `alpha` and `beta` typically sit above 1.0,
  bracketing the workload's natural spread.

The defaults — `alpha = 1.30`, `beta = 1.80`, baseline at p10 and
current at p50 — put the controller in cross mode with the hold band
straddling the typical p10/p50 spread of metadata syscalls. At steady
state the ratio sits inside `[alpha, beta]` and `cwnd` holds; the
controller grows when the spread compresses (the recent distribution
is unusually concentrated near its lower decile, i.e. headroom) and
shrinks when queueing fattens the upper tail past `beta`. Workloads
whose idle p10/p50 spread sits well outside this range — extremely
flat distributions or unusually skewed metadata paths — should
re-bracket `alpha` / `beta` around the observed idle ratio.

A few worked examples make the shape concrete. Assume a 0.5 ms
baseline percentile (p10), default thresholds, and `cwnd = 20`
going into the tick:

| Current percentile (p50) | Ratio | Decision | New `cwnd` |
|--------------------------|-------|----------|------------|
| 0.50 ms                  | 1.00  | grow     | 21 (degenerate distribution; spread has compressed) |
| 0.60 ms                  | 1.20  | grow     | 21         |
| 0.65 ms                  | 1.30  | hold     | 20 (1.30 == alpha; not below) |
| 0.75 ms                  | 1.50  | hold     | 20         |
| 0.90 ms                  | 1.80  | hold     | 20 (1.80 == beta; not above) |
| 1.00 ms                  | 2.00  | shrink   | 19         |
| 2.50 ms                  | 5.00  | shrink   | 19         |
| 0.45 ms                  | 0.90  | grow     | 21 (recent median below long-horizon decile — strong improvement) |

**Two things to notice in this table.** First, the law is a
*binary trigger*, not a proportional response: a ratio of 5.0 shrinks
by exactly the same one step as a ratio of 1.81. Sustained inflation
drives faster *effective* shrinkage because the ratio stays above
`beta` over many consecutive ticks — but each individual tick still
takes one step. This binary-trigger shape, inherited from TCP Vegas,
keeps the control law simple and less prone to oscillation under
noisy latency than a gain-tuned PID.

Second, the natural steady-state ratio is well above 1.0 because the
defaults are cross (p10 baseline, p50 current). A ratio below 1.0 is
unusual — it means the recent median is faster than the long-horizon
p10, which only happens during a clear improvement (e.g. a competing
client dropped off). The controller treats it as growth headroom.

The "responsiveness" of the controller is shaped by these knobs in
combination:

- `--auto-meta-tick-interval` (how often the binning above is
  evaluated; default 50 ms);
- `--auto-meta-baseline-percentile` /
  `--auto-meta-current-percentile` (the percentiles used in the
  long and short windows; defaults 0.1 / 0.5 — cross p10/p50);
- `--auto-meta-long-window` / `--auto-meta-short-window` (how much
  history is used for each estimate; defaults 10s / 1s);
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
- **Not probed by design — directory iteration.** Walks (`next_entry`
  + cached `file_type`) are *not* probed and *not* concurrency-capped.
  `tokio::fs::ReadDir` amortizes a single `getdents` syscall over many
  `next_entry` calls, so most "walk probes" don't enter the kernel at
  all and complete in tens of nanoseconds. The resulting bimodal
  cache-hit / real-syscall distribution collapses any baseline a
  controller could derive from it and pins `cwnd` at the floor. The
  per-file metadata syscalls that *follow* the walk still carry a
  clean signal, so we let those drive the controllers and skip the
  walk path entirely. One side-effect worth flagging: a workload that
  walks a wide tree but filters most entries away (large
  `--include`/`--exclude` exclusions) will still spawn concurrent
  directory scans without any auto-meta backpressure, since the
  filter short-circuit happens before any metadata syscall fires.
  Walks self-pace through the OS getdents cache and the global
  `--ops-throttle` rate gate still applies; if you need a hard cap on
  walk-side load on a fragile NAS, reach for `--ops-throttle`.
- **Not probed by design — data path.** The read-loop and write-loop
  inside the copy pipeline, and `tokio::fs::copy` itself. Bandwidth-
  bound, not service-time-bound; a latency-ratio controller doesn't
  fit. See [Pluggability](#pluggability) for the BBR direction.

## One Controller per (Side, Syscall)

Filesystems vary enormously, and so do individual metadata syscalls.
A copy from a saturated NFS mount to a local SSD has two completely
independent service-time profiles for the two sides; within each side,
`stat` (a pure lookup), `unlink` (a parent-directory write), and
`mkdir` (an inode allocation) hit different code paths on the metadata
server and have different baseline latencies. Mixing them in a single
controller pollutes the per-op signal and makes the percentile-ratio
baseline drift with operation-mix changes that have nothing to do
with congestion.

So we run **one controller per `(Side, MetadataOp)` pair** — up to 18
in total. Each carries its own sample window, its own baseline /
current percentiles, its own `cwnd`, and its own enforcement
semaphore. A probe site is annotated with both the side and the op
kind (`Metadata(Source, Stat)`, `Metadata(Destination, Unlink)`, …);
the control loop and enforcement gate are picked accordingly.

The covered op kinds:

| Op            | Used for                                                   |
|---------------|------------------------------------------------------------|
| `Stat`        | `symlink_metadata`, `metadata`, `canonicalize`, read-only `File::open` |
| `ReadLink`    | `read_link`                                                |
| `MkDir`       | `create_dir`                                               |
| `RmDir`       | `remove_dir`                                               |
| `Unlink`      | `remove_file`                                              |
| `HardLink`    | `hard_link`                                                |
| `Symlink`     | `symlink` (creation)                                       |
| `Chmod`       | `set_permissions`, `chown`/`fchownat`, `utimes`/`utimensat` |
| `OpenCreate`  | `File::create`, `OpenOptions::open(create=true, …)`        |

Sources are immutable in copy/cmp/link/rm, so the mutation ops
(`MkDir`, `RmDir`, `Unlink`, `HardLink`, `Symlink`, `Chmod`,
`OpenCreate`) only ever fire on the destination side. The lookup ops
(`Stat`, `ReadLink`) can fire on either side. The progress-bar
labelling reflects this: lookup labels carry an explicit `src-` /
`dst-` prefix to disambiguate (`src-stat`, `dst-stat`,
`dst-read-link`); mutation labels drop the prefix entirely
(`mkdir`, `unlink`, `rmdir`, `hard-link`, `symlink`, `chmod`,
`open-create`).

How tools map onto this:

| Tool      | Active controllers (typical)                                                          |
|-----------|---------------------------------------------------------------------------------------|
| `rcp`     | `src-stat`, `src-read-link`, `dst-stat`, `mkdir`, `symlink`, `open-create`, `chmod`, `rmdir`, `unlink` |
| `rrm`     | `src-stat`, `unlink`, `rmdir`                                                         |
| `rlink`   | `src-stat`, `src-read-link`, `dst-stat`, `mkdir`, `hard-link`, `symlink`, `chmod`     |
| `rcmp`    | `src-stat`, `dst-stat` (compare-only — no writes)                                     |
| `filegen` | `mkdir`, `open-create`, `chmod`                                                       |

Every tool registers all 18 controllers uniformly; controllers it
doesn't exercise stay at `samples_seen = 0` and the renderer hides
them, so users only see the labels their workload actually drove.

A note on the rate dimension: each controller gates its own
in-flight concurrency independently, but the global ops-throttle
(rate-per-second) is shared across the process. Only the
`(Destination, Stat)` controller drives that single rate gate by
convention; the others apply concurrency only. The current
`RatioController` doesn't emit rate decisions, so this choice is
forward-looking — if a rate-aware controller (BBR-style, …) is
swapped in later, exactly one of them must own the global rate gate.

In remote copy, this layout maps cleanly: `rcpd-source` exercises only
the source-side stat / read-link controllers (only ever reads its
local source filesystem), and `rcpd-destination` exercises the
destination-side stat / read-link controllers plus all the mutation
controllers (only ever mutates its local destination filesystem). The
unused channels stay idle on each side.

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
and maximum cwnd, the grow/shrink thresholds, the baseline and current
percentiles, the long / short window durations, per-tick step sizes,
and the control-loop tick cadence. Defaults are conservative;
aggressive-but-sensible tuning comes from field measurements.

The control loop emits structured `tracing` events on a few channels —
each tagged with a `unit` field so the per-syscall controllers can be
told apart in mixed logs (`src-stat`, `dst-stat`, `mkdir`, `unlink`,
…; see [One Controller per (Side, Syscall)](#one-controller-per-side-syscall)
for the full mapping):

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
- **Latency-ratio** (`RatioController`): the adaptive controller
  described above. Inspired by TCP Vegas's
  current-vs-baseline-RTT signal, with two extensions: each window
  is summarized by a configurable percentile rather than by min/mean,
  and the baseline and current percentiles can differ to encode the
  inter-quantile spread of the latency distribution as the queueing
  signal. Adjacent precedents in the wider literature: CoDel uses
  a single-percentile (min) target detector; BBR tracks windowed
  max bandwidth and min RTT.

New algorithms plug into the same enforcement machinery and the same
simulator. BBR-style estimators (tracking bottleneck bandwidth and
round-trip propagation separately) are a natural next step for the
remote copy protocol, where the network and the filesystem contribute
different latency components.
