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

**Control loop.** Async glue. Each controlled resource (metadata,
read-throughput, write-throughput) has its own instance, running as a
lightweight task. It drains a bounded sample queue, calls the
algorithm's tick on a configurable cadence, and publishes each emitted
decision to the enforcement layer.

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

- **Baseline latency** — the minimum operation latency we've seen
  recently. By construction this is the floor; no individual sample can
  be faster.
- **Smoothed current latency** — an EWMA of recent operation latencies.
  Smoothing absorbs single-sample noise so a brief slow op doesn't kick
  the controller off course.

Their ratio is the congestion signal:

```
  ratio  =  smoothed_current / baseline
```

By construction `ratio >= 1.0` (the smoothed current can't be smaller
than the running minimum that defines the baseline). So the regime we
care about is the strip between 1.0 and "very large":

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

1. **The baseline is a running minimum, not an average.** `min_latency`
   records the *smallest* sample observed in the last
   `min_latency_max_age` window (default 10s). Even when 99% of samples
   in that window are slow because we're saturating the filesystem, a
   single fast sample — typically from earlier, before `cwnd` ramped
   high enough to queue — pins the floor to the true uncongested cost.
   Slow samples can never drag the floor up; they simply aren't
   smaller.

2. **The control law shrinks `cwnd` long before the window expires.**
   When `ratio > beta`, the very next tick (50 ms by default) shrinks
   `cwnd` by one step. Continued inflation keeps shrinking it.  Once
   `cwnd` has dropped enough for the queue to drain, a low-latency
   sample either re-confirms the existing minimum (refreshing the
   age-out timer) or establishes a new, lower one. So in any healthy
   operating regime the baseline is continuously re-validated long
   before it could expire.

3. **If the baseline does expire under sustained saturation, the
   smoothed estimate is reset alongside it.** The case left open is
   *persistent* overload — e.g. `min_cwnd` is configured high and the
   filesystem is genuinely overloaded for more than 10 seconds. When
   the baseline ages out, the next sample establishes a new (inflated)
   floor. To prevent that newly-bad baseline from making the next tick
   look "uncongested" and grow `cwnd` further, the EWMA is reset at
   the same instant. The first sample-bearing tick after a reset
   replays the cold-start path and never adjusts `cwnd`, buying a full
   smoothing window for the controller to reconverge before any growth
   decision.

The only state the controller carries across a baseline reset is
`cwnd` itself — intentionally. The next ratio is computed from a fresh
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

Defaults: `alpha = 1.10`, `beta = 1.50`, `increase_step = decrease_step
= 1`. Each step is then clamped to `[min_cwnd, max_cwnd]`.

A few worked examples make the shape concrete. Assume a 0.5ms baseline,
default thresholds, and `cwnd = 20` going into the tick:

| Smoothed current | Ratio | Decision | New `cwnd` |
|------------------|-------|----------|------------|
| 0.50 ms          | 1.00  | grow     | 21         |
| 0.55 ms          | 1.10  | hold     | 20 (1.10 == alpha; not below) |
| 0.70 ms          | 1.40  | hold     | 20         |
| 0.75 ms          | 1.50  | hold     | 20 (1.50 == beta; not above) |
| 1.00 ms          | 2.00  | shrink   | 19         |
| 2.50 ms          | 5.00  | shrink   | 19         |
| 0.40 ms          | 0.80  | —        | impossible (see below) |

**Two things to notice in this table.** First, the law is a
*binary trigger*, not a proportional response: a ratio of 5.0 shrinks
by exactly the same one step as a ratio of 1.51. Sustained inflation
drives faster *effective* shrinkage because the ratio stays above
`beta` over many consecutive ticks — but each individual tick still
takes one step. This is the classic Vegas shape: simpler control law,
less prone to oscillation under noisy latency than a gain-tuned PID.

Second, **a ratio below 1.0 cannot occur.** The smoothed current is
itself an EWMA of samples that have already been folded into the
running min, so it is bounded below by that min. The baseline-age-out
path preserves this property: when the min is discarded as stale, the
smoothed current is reset alongside it so the next tick re-establishes
both from the same fresh window of samples.

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

Three observability signals are worth watching when experimenting:

- **Tracing events at `info!`** when the controller starts, describing
  the effective configuration.
- **Warning at `warn!`** if the internal sample queue drops events —
  typically a signal that the tick cadence or queue capacity is
  misconfigured for the workload's rate.
- **Debug trace** when the control loop exits, so operators can confirm
  clean shutdown vs. a stuck task.

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
