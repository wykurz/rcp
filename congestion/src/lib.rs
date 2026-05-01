//! Pluggable congestion control for the RCP tool family.
//!
//! This crate exposes a [`Controller`] trait that encapsulates a
//! congestion-control algorithm — consuming operation-completion
//! [`Sample`]s and emitting concurrency / rate [`Decision`]s. The trait is
//! intentionally small and synchronous so algorithms can be implemented and
//! tested without any I/O, async runtime, or wall-clock dependence.
//!
//! Three concentric layers are envisioned for integration with the rcp /
//! rrm / rlink / filegen tools:
//!
//! 1. **Algorithm** — the `Controller` trait and its implementations; pure.
//! 2. **Control loop** — plumbing that collects samples from the hot path,
//!    drives the controller on a fixed tick, and publishes decisions. Lives
//!    outside this crate (in `throttle` or a thin adapter) and is not part
//!    of Phase 1.
//! 3. **Enforcement** — the existing `throttle` token buckets and
//!    semaphores, with their limits driven by the control loop.
//!
//! # Built-in controllers
//!
//! - [`NoopController`] — never limits. Default when congestion control is
//!   disabled.
//! - [`FixedController`] — honors a static concurrency/rate budget. Mirrors
//!   the existing manual `--ops-throttle` / `--iops-throttle` knobs and is
//!   the regression baseline for adaptive algorithms.
//! - [`RatioController`] — adaptive controller that tracks queueing-delay
//!   inflation by comparing two windowed latency percentiles (current vs
//!   baseline) and adjusts the concurrency cap to stay at the onset of
//!   inflation. Inspired by TCP Vegas.
//!
//! Additional adaptive variants (for example BBR-style) can be layered
//! on the same trait without changes to the enforcement or control-loop
//! layers.
//!
//! # Testing
//!
//! The [`sim`] module provides a deterministic single-bottleneck simulator
//! that drives any `Controller` through a configured scenario and returns a
//! trace of samples and decisions. See the module docs for the model.

mod control_loop;
mod controller;
mod fixed;
mod measurement;
mod noop;
mod ratio;
pub mod sim;
pub mod testing;

pub use control_loop::{ControlUnit, DEFAULT_TICK_INTERVAL, RoutingSink, RoutingSinkBuilder};
pub use controller::{Controller, ControllerSnapshot, Decision, Outcome, Sample};
pub use fixed::FixedController;
pub use measurement::{
    MetadataOp, N_META_OPS, N_META_RESOURCES, N_SIDES, Probe, ResourceKind, SampleSink, Side,
    clear_sample_sink, install_sample_sink,
};
pub use noop::NoopController;
pub use ratio::{RatioConfig, RatioController};
