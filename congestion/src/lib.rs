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
//!
//! Adaptive controllers (Vegas-style, BBR-style) are planned on this same
//! trait in later phases.
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
pub mod sim;
pub mod testing;
mod vegas;

pub use control_loop::{ControlUnit, DEFAULT_TICK_INTERVAL, RoutingSink, RoutingSinkBuilder};
pub use controller::{Controller, Decision, Outcome, Sample};
pub use fixed::FixedController;
pub use measurement::{Probe, ResourceKind, SampleSink, clear_sample_sink, install_sample_sink};
pub use noop::NoopController;
pub use vegas::{VegasConfig, VegasController};
