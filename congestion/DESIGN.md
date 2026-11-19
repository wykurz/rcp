# Congestion Control Design

`congestion` owns the metadata-latency controller, sample routing, and decision stream. It does not
perform filesystem operations or enforce limits itself. Start with the
[operator flag guide](../README.md#adaptive-metadata-throttling---auto-meta-throttle) for usage;
[the detailed reference](../docs/congestion_control.md) retains the control law, tuning rationale,
and histogram V2 format.

## Control loop

```
┌───────────────────────────┐
│         Algorithm         │
│ (controller trait + impl) │
│   sample in → decision    │
└─────────^─────────┬───────┘
          │         │
  samples │         │ decisions
          │         v
┌─────────┴─────────────────┐
│       Control Loop        │
│  sample routing + tick +  │
│      decision broadcast   │
└─────────^─────────┬───────┘
          │         │
  probes  │         │ limits
          │         v
┌─────────┴─────────────────┐
│        Enforcement        │
│  concurrency semaphores + │
│     rate token buckets    │
└─────────^─────────────────┘
          │
          │ acquire / release
          │
┌─────────┴─────────────────┐
│       Filesystem Ops      │
│  getdents, stat, open,    │
│      read, write, …       │
└───────────────────────────┘
```

The diagram shows the integration boundary, not a network congestion controller. Today the adaptive
policy controls metadata concurrency, not data-transfer bandwidth. Each process has independent
controllers for each `(filesystem side, metadata syscall)` pair. Remote copies run these controllers
in the source and destination `rcpd`, not the coordinating master.

## Ownership and enforcement map

| Layer               | Entry points                                                                                             | Responsibility                                                                                                        |
| ------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| Algorithm           | [`controller.rs`](src/controller.rs), [`ratio.rs`](src/ratio.rs)                                         | Synchronous samples-to-decisions policy; time is supplied by the caller.                                              |
| Sampling and loop   | [`measurement.rs`](src/measurement.rs), [`control_loop.rs`](src/control_loop.rs)                         | Probe completions, bounded sample routing, periodic ticks, decision publication.                                      |
| Runtime wiring      | [`runtime_setup.rs`](../common/src/runtime_setup.rs)                                                     | Construct per-resource controllers, install the sample sink, start adapters and optional histogram logging.           |
| Enforcement adapter | [`auto_meta.rs`](../common/src/auto_meta.rs)                                                             | Apply decisions to the matching throttle resource; only the destination-stat adapter drives the shared ops-rate gate. |
| Filesystem gates    | [`walk.rs`](../common/src/walk.rs), [`throttle`](../throttle/src/lib.rs)                                 | Acquire permits before metadata work; measure execution after admission, then release permits.                        |
| Observability       | [`histogram_logger.rs`](../common/src/histogram_logger.rs), [`progress rendering`](../common/src/lib.rs) | Snapshot distributions and progress counters; write logs or render the local registry.                                |
| Remote policy       | [`rcp.rs`](../rcp/src/bin/rcp.rs), [`RcpdConfig`](../remote/src/protocol/mod.rs)                         | Forward explicit throttle/log intent, not a panel-only request; keep histogram files on the endpoint hosts.           |

## Boundaries worth preserving

- Admission wait is excluded from metadata latency samples. Otherwise the controller would respond
  to its own gate instead of the filesystem work it is trying to regulate.
- Each decision targets its own resource. The shared ops-rate gate has one designated adapter,
  avoiding competing writers; static and adaptive limits remain distinct dimensions.
- The live histogram panel reads a process-local registry. Remote histogram snapshots are not
  transported to the master, so an empty remote panel is not evidence of missing filesystem work.
- Histogram collection is not an observation-only switch for local copies or remote log capture:
  those modes enable adaptive throttling. The remote panel-only exception is intentional. See the
  [operator behavior table](../README.md#adaptive-metadata-throttling---auto-meta-throttle).

## Tests and detailed reference

[`sim.rs`](src/sim.rs) and [`testing.rs`](src/testing.rs) exercise policies without filesystem I/O.
Integration coverage belongs with the runtime wiring, adapter, CLI, and remote configuration. Use
the repository [testing](../docs/testing.md) and [benchmarking](../docs/benchmarking.md) guides for
verification and measurement rather than inferring performance from simulator output.

Keep the [control law](../docs/congestion_control.md#the-control-law),
[enforcement details](../docs/congestion_control.md#enforcement-model), and
[V2 log layout](../docs/congestion_control.md#log-file-layout) in the detailed reference so existing
links and log readers have one stable target.
