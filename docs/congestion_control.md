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
- [The Control Signal](#the-control-signal)
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
                        │  (controller trait + impl) │
                        │   sample in → decision    │
                        └─────────▲─────────┬───────┘
                                  │         │
                          samples │         │ decisions
                                  │         ▼
                        ┌─────────┴─────────────────┐
                        │       Control Loop        │
                        │  sample routing + tick +  │
                        │      decision broadcast    │
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

## The Control Signal

The algorithm's job is to find a concurrency level that saturates the
bottleneck without queueing. It reasons about two derived quantities:

- **Baseline latency.** The minimum operation latency we've seen
  recently — an estimate of what the filesystem costs per op when
  uncongested.
- **Smoothed current latency.** An exponentially-weighted moving average
  of recent operation latencies.

Their ratio is the congestion signal:

```
       latency ratio
              │
  beta ──────┤                ┌── shrink cwnd ──┐
              │               │                 │
              │               │                 │
  alpha ─────┤    ┌── hold ──┤
              │   │           
    1.0  ────┤───┘                              
              │   grow cwnd                     
              │                                 
              └───────────────────────────────── time
```

- Below `alpha` (e.g. 1.1× baseline): we're under-utilized; grow `cwnd`.
- Above `beta` (e.g. 1.5× baseline): the queue is building; shrink
  `cwnd`.
- Between: hold.

The baseline ages out after a configurable interval so a single stale
low-latency outlier can't pin the controller at the floor forever. When
the baseline ages out, the smoothed-latency estimate is reset alongside
it so the newly-established baseline is compared to a fresh window of
samples rather than to inflated state carried over from a congested
period.

## Enforcement Model

The controller's output is an absolute *concurrency cap* — the maximum
number of filesystem operations that may be in flight at any moment.
This maps onto a semaphore whose capacity is updated whenever the
controller changes its mind.

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
            │     (sample emitted)                │
            │                                     │
            └──── release permit ────────────────┘
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
              rate gate         concurrency gate
              (token             (semaphore)
              bucket)            
    caller ───▶ [  ] ──────▶ [    ] ────▶ syscall
                ▲                ▲
                │                │
       --ops-throttle    --auto-meta-throttle
       (static rate)     (dynamic concurrency)
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
