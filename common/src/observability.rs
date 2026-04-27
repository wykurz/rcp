//! Process-wide registry of congestion-control snapshot streams.
//!
//! When the auto-meta-throttle setup spawns a `ControlUnit`, it
//! [`register_unit`]s the unit's snapshot watch receiver. Renderers (the
//! progress bar, text-update mode, future telemetry exporters) call
//! [`registered_units`] to enumerate the active controllers and read
//! their latest snapshots without subscribing to the underlying watch.
//!
//! The registry is empty unless adaptive control is active; non-adaptive
//! runs see an empty list and can render a plain progress bar.

use congestion::ControllerSnapshot;

/// One entry in the registry: the unit's stable label plus a watch
/// receiver for its snapshot stream. The receiver is cheap to clone
/// (one ref-count bump), so renderers can pull the latest value
/// non-blockingly with `borrow()`.
#[derive(Clone)]
pub struct RegisteredUnit {
    pub label: &'static str,
    pub snapshot_rx: tokio::sync::watch::Receiver<ControllerSnapshot>,
}

static REGISTRY: std::sync::LazyLock<std::sync::RwLock<Vec<RegisteredUnit>>> =
    std::sync::LazyLock::new(|| std::sync::RwLock::new(Vec::new()));

/// Register a unit's snapshot stream with the process-wide registry.
///
/// Called once per spawned `ControlUnit`. Order is preserved — renderers
/// display units in registration order, so callers should register in
/// the order they want lines to appear.
pub fn register_unit(
    label: &'static str,
    snapshot_rx: tokio::sync::watch::Receiver<ControllerSnapshot>,
) {
    REGISTRY
        .write()
        .expect("observability registry poisoned")
        .push(RegisteredUnit { label, snapshot_rx });
}

/// Snapshot of the current registry. Cheap (clones the inner Vec of
/// `Arc`-backed receivers); intended to be called once per
/// progress-render tick.
#[must_use]
pub fn registered_units() -> Vec<RegisteredUnit> {
    REGISTRY
        .read()
        .expect("observability registry poisoned")
        .clone()
}

/// Drop all registered units. Called from the process-wide reset path
/// in `crate::run` so a second invocation of `run()` in the same
/// process starts with a clean registry.
pub fn clear() {
    REGISTRY
        .write()
        .expect("observability registry poisoned")
        .clear();
}

/// Render the registered units as a multi-line block suitable for
/// appending to the progress display. Returns an empty string if no
/// units are registered (so non-adaptive runs render an unmodified
/// progress bar).
///
/// The format is one fixed-width line per unit:
///
/// ```text
/// walk-src cwnd=42  base=0.8ms  ewma=2.1ms  ratio=2.6×  samples=1.2k
/// ```
///
/// Unit labels are padded to a uniform width so columns align even
/// across mixed `walk-` / `meta-` names. Each line begins with a
/// newline so it composes cleanly when appended to an existing
/// message.
#[must_use]
pub fn render_lines() -> String {
    let units = registered_units();
    if units.is_empty() {
        return String::new();
    }
    let label_width = units.iter().map(|u| u.label.len()).max().unwrap_or(0);
    let mut out = String::new();
    for unit in &units {
        let snap = *unit.snapshot_rx.borrow();
        out.push('\n');
        out.push_str(&format_unit_line(unit.label, label_width, snap));
    }
    out
}

fn format_unit_line(label: &str, label_width: usize, snap: ControllerSnapshot) -> String {
    let ratio = if snap.min_latency.is_zero() {
        // No baseline yet — no meaningful ratio to display. Match the
        // "—" convention used elsewhere when a metric isn't yet
        // populated.
        String::from("—")
    } else {
        let ratio = snap.ewma_latency.as_nanos() as f64 / snap.min_latency.as_nanos() as f64;
        format!("{ratio:.1}×")
    };
    format!(
        "{label:<width$}  cwnd={cwnd:<4}  base={base}  ewma={ewma}  ratio={ratio:<5}  samples={samples}",
        label = label,
        width = label_width,
        cwnd = snap.cwnd,
        base = format_duration(snap.min_latency),
        ewma = format_duration(snap.ewma_latency),
        ratio = ratio,
        samples = format_count(snap.samples_seen),
    )
}

/// Compact, uniform-width latency formatter. Picks the unit so the
/// number stays in 1–3 digits ("0.8ms", "12ms", "1.4s") to keep
/// columns roughly aligned.
fn format_duration(d: std::time::Duration) -> String {
    if d.is_zero() {
        return String::from("  —  ");
    }
    let nanos = d.as_nanos();
    if nanos < 1_000 {
        format!("{nanos}ns")
    } else if nanos < 1_000_000 {
        format!("{:.1}µs", nanos as f64 / 1_000.0)
    } else if nanos < 1_000_000_000 {
        format!("{:.1}ms", nanos as f64 / 1_000_000.0)
    } else {
        format!("{:.1}s", nanos as f64 / 1_000_000_000.0)
    }
}

/// Compact thousands formatting — `1234` → `"1.2k"`, `1_500_000` →
/// `"1.5M"`. Saturates at 'G' (10^9) which is plenty for sample counts.
fn format_count(n: u64) -> String {
    if n < 1_000 {
        n.to_string()
    } else if n < 1_000_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else if n < 1_000_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else {
        format!("{:.1}G", n as f64 / 1_000_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The registry is global, so these tests serialize via this guard
    /// to avoid stepping on each other when run concurrently.
    static GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn empty_registry_returns_empty_vec() {
        let _g = GUARD.lock().unwrap();
        clear();
        assert!(registered_units().is_empty());
    }

    #[test]
    fn registered_units_preserve_insertion_order() {
        let _g = GUARD.lock().unwrap();
        clear();
        let (_tx_a, rx_a) = tokio::sync::watch::channel(ControllerSnapshot::default());
        let (_tx_b, rx_b) = tokio::sync::watch::channel(ControllerSnapshot::default());
        register_unit("first", rx_a);
        register_unit("second", rx_b);
        let units = registered_units();
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].label, "first");
        assert_eq!(units[1].label, "second");
        clear();
    }

    #[test]
    fn snapshot_updates_visible_via_registered_receiver() {
        let _g = GUARD.lock().unwrap();
        clear();
        let (tx, rx) = tokio::sync::watch::channel(ControllerSnapshot::default());
        register_unit("only", rx);
        let new_snapshot = ControllerSnapshot {
            cwnd: 42,
            ..ControllerSnapshot::default()
        };
        tx.send(new_snapshot).expect("send snapshot");
        let units = registered_units();
        assert_eq!(units[0].snapshot_rx.borrow().cwnd, 42);
        clear();
    }

    #[test]
    fn render_lines_is_empty_when_registry_is_empty() {
        let _g = GUARD.lock().unwrap();
        clear();
        assert_eq!(render_lines(), "");
    }

    #[test]
    fn render_lines_shows_one_line_per_unit_with_aligned_labels() {
        let _g = GUARD.lock().unwrap();
        clear();
        let (_tx_a, rx_a) = tokio::sync::watch::channel(ControllerSnapshot {
            cwnd: 8,
            min_latency: std::time::Duration::from_micros(800),
            ewma_latency: std::time::Duration::from_millis(2),
            samples_seen: 1234,
        });
        let (_tx_b, rx_b) = tokio::sync::watch::channel(ControllerSnapshot {
            cwnd: 16,
            min_latency: std::time::Duration::from_millis(1),
            ewma_latency: std::time::Duration::from_millis(3),
            samples_seen: 5678,
        });
        register_unit("walk-src", rx_a);
        register_unit("meta-dst", rx_b);
        let out = render_lines();
        let lines: Vec<&str> = out.split('\n').filter(|s| !s.is_empty()).collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("walk-src"));
        assert!(lines[0].contains("cwnd=8"));
        // EWMA / min_latency = 2ms / 800µs = 2.5×
        assert!(lines[0].contains("ratio=2.5×"));
        assert!(lines[0].contains("samples=1.2k"));
        assert!(lines[1].contains("meta-dst"));
        assert!(lines[1].contains("cwnd=16"));
        assert!(lines[1].contains("samples=5.7k"));
        clear();
    }

    #[test]
    fn render_lines_shows_em_dash_when_baseline_unset() {
        let _g = GUARD.lock().unwrap();
        clear();
        let (_tx, rx) = tokio::sync::watch::channel(ControllerSnapshot::default());
        register_unit("walk-src", rx);
        let out = render_lines();
        // ratio is "—" when no baseline yet, prevents a 0/0 division.
        assert!(out.contains("ratio=—"));
        clear();
    }
}
