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

/// Width (in display chars) of every right-aligned numeric column in
/// the rendered panel. Wide enough to fit the realistic worst cases
/// without truncation: `999.9µs`, `1234.5×`, `999.9k`.
const FIELD_WIDTH: usize = 7;

/// Section separator drawn above the auto-meta panel. Matches the
/// dashed style used elsewhere in the progress printers so the panel
/// reads as just another section break rather than free-floating text.
const SEPARATOR: &str = "-----------------------";

/// Render the registered units as a multi-line block suitable for
/// appending to the progress display. Returns an empty string when
/// either (a) no units are registered (non-adaptive run) or (b) every
/// registered unit has zero samples — typically a brief startup window
/// before the first probe lands, or a unit class that the current tool
/// never exercises (e.g. `mkdir` for `rcmp`, which only stats both
/// sides). With per-syscall controllers we register up to 18 units
/// (Side × MetadataOp); per-tool only a handful actually fire probes
/// and the rest stay hidden via this `samples_seen > 0` filter.
///
/// The format is one fixed-width line per unit, prefixed by a dashed
/// separator so the panel sits visually apart from the COPIED/REMOVED/
/// SKIPPED sections above it:
///
/// ```text
/// -----------------------
/// src-stat   cwnd=  42  base=  0.8ms  curr=  2.1ms  ratio=   2.6×  samples=   1.2k
/// unlink     cwnd=  18  base=  1.2ms  curr=  3.0ms  ratio=   2.5×  samples= 980.0
/// rmdir      cwnd=   4  base=  2.4ms  curr=  6.1ms  ratio=   2.5×  samples=  80.0
/// ```
///
/// Unit labels are padded to a uniform width so columns align even
/// across labels of varying length (`stat`, `dst-read-link`, `open-create`).
/// Numeric columns are right-aligned to a uniform fixed width.
#[must_use]
pub fn render_lines() -> String {
    let units = registered_units();
    if units.is_empty() {
        return String::new();
    }
    // Snapshot once per render so a probe completing mid-render can't
    // make a row appear/disappear between the empty check and the loop.
    let snapshots: Vec<(&'static str, ControllerSnapshot)> = units
        .iter()
        .map(|u| (u.label, *u.snapshot_rx.borrow()))
        .collect();
    let visible: Vec<(&'static str, ControllerSnapshot)> = snapshots
        .into_iter()
        .filter(|(_, snap)| snap.samples_seen > 0)
        .collect();
    if visible.is_empty() {
        return String::new();
    }
    let label_width = visible.iter().map(|(l, _)| l.len()).max().unwrap_or(0);
    let mut out = String::new();
    out.push('\n');
    out.push_str(SEPARATOR);
    for (label, snap) in &visible {
        out.push('\n');
        out.push_str(&format_unit_line(label, label_width, *snap));
    }
    out
}

fn format_unit_line(label: &str, label_width: usize, snap: ControllerSnapshot) -> String {
    let ratio = if snap.baseline_latency.is_zero() || snap.current_latency.is_zero() {
        // Either statistic missing → no meaningful ratio. The
        // controller surfaces both as `Duration::ZERO` in the snapshot
        // when its underlying `Option<u64>` was `None` (e.g. an empty
        // short window holds cwnd but leaves the current statistic
        // unset). Treat both as the unset sentinel and emit "—" rather
        // than rendering `ratio=0.0×`, which would imply an actual
        // faster-than-baseline reading.
        String::from("—")
    } else {
        let ratio =
            snap.current_latency.as_nanos() as f64 / snap.baseline_latency.as_nanos() as f64;
        format!("{ratio:.1}×")
    };
    format!(
        "{label:<lwidth$}  cwnd={cwnd:>4}  base={base:>fwidth$}  curr={curr:>fwidth$}  ratio={ratio:>fwidth$}  samples={samples:>fwidth$}",
        label = label,
        lwidth = label_width,
        fwidth = FIELD_WIDTH,
        cwnd = snap.cwnd,
        base = format_duration(snap.baseline_latency),
        curr = format_duration(snap.current_latency),
        ratio = ratio,
        samples = format_count(snap.samples_seen),
    )
}

/// Compact latency formatter. Picks the unit so the number stays in
/// 1–4 chars (`58`, `1.7`, `33.5`, `999.9`); the outer format string
/// pads the result to [`FIELD_WIDTH`] so consecutive rows line up.
fn format_duration(d: std::time::Duration) -> String {
    if d.is_zero() {
        return String::from("—");
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
            baseline_latency: std::time::Duration::from_micros(800),
            current_latency: std::time::Duration::from_millis(2),
            samples_seen: 1234,
        });
        let (_tx_b, rx_b) = tokio::sync::watch::channel(ControllerSnapshot {
            cwnd: 16,
            baseline_latency: std::time::Duration::from_millis(1),
            current_latency: std::time::Duration::from_millis(3),
            samples_seen: 5678,
        });
        register_unit("walk-src", rx_a);
        register_unit("meta-dst", rx_b);
        let out = render_lines();
        let lines: Vec<&str> = out.split('\n').filter(|s| !s.is_empty()).collect();
        // Separator + 2 unit rows.
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], SEPARATOR);
        assert!(lines[1].contains("walk-src"));
        // cwnd is right-aligned to 4 chars; numeric columns to FIELD_WIDTH.
        assert!(lines[1].contains("cwnd=   8"));
        // current / baseline = 2ms / 800µs = 2.5×
        assert!(lines[1].contains("ratio=   2.5×"));
        assert!(lines[1].contains("samples=   1.2k"));
        assert!(lines[2].contains("meta-dst"));
        assert!(lines[2].contains("cwnd=  16"));
        assert!(lines[2].contains("samples=   5.7k"));
        clear();
    }

    #[test]
    fn render_lines_skips_units_with_zero_samples() {
        // Tools that don't exercise a side (e.g. rrm never walks the
        // destination tree) leave that controller's `samples_seen` at
        // zero. We don't show the row at all rather than render a
        // permanent placeholder of dashes.
        let _g = GUARD.lock().unwrap();
        clear();
        let (_tx_a, rx_a) = tokio::sync::watch::channel(ControllerSnapshot {
            cwnd: 8,
            baseline_latency: std::time::Duration::from_micros(800),
            current_latency: std::time::Duration::from_millis(2),
            samples_seen: 1234,
        });
        let (_tx_b, rx_b) = tokio::sync::watch::channel(ControllerSnapshot {
            cwnd: 1,
            baseline_latency: std::time::Duration::ZERO,
            current_latency: std::time::Duration::ZERO,
            samples_seen: 0,
        });
        register_unit("walk-src", rx_a);
        register_unit("walk-dst", rx_b);
        let out = render_lines();
        assert!(out.contains("walk-src"));
        assert!(!out.contains("walk-dst"));
        clear();
    }

    #[test]
    fn render_lines_is_empty_when_all_units_have_zero_samples() {
        // At startup, before any probes have completed, every controller
        // reports samples_seen = 0. The panel shouldn't render a bare
        // separator with nothing under it.
        let _g = GUARD.lock().unwrap();
        clear();
        let (_tx, rx) = tokio::sync::watch::channel(ControllerSnapshot::default());
        register_unit("walk-src", rx);
        assert_eq!(render_lines(), "");
        clear();
    }

    #[test]
    fn render_lines_shows_em_dash_when_baseline_unset() {
        // It's possible (briefly) for a unit to have samples_seen > 0
        // but the published snapshot's baseline_latency still at zero, if
        // the snapshot was captured between on_sample and the first
        // sample-bearing on_tick. Guard against the resulting 0/0 by
        // emitting "—" for ratio.
        let _g = GUARD.lock().unwrap();
        clear();
        let (_tx, rx) = tokio::sync::watch::channel(ControllerSnapshot {
            cwnd: 1,
            baseline_latency: std::time::Duration::ZERO,
            current_latency: std::time::Duration::ZERO,
            samples_seen: 1,
        });
        register_unit("walk-src", rx);
        let out = render_lines();
        assert!(out.contains("ratio="));
        assert!(out.contains("—"));
        clear();
    }

    #[test]
    fn render_lines_shows_em_dash_when_only_current_unset() {
        // Regression: when the long window has samples but the short
        // window is empty (a common state on ticks where the activity
        // gap exceeds short_window), the controller publishes a
        // populated baseline_latency and `current_latency =
        // Duration::ZERO`. The renderer must treat that as the unset
        // sentinel and emit "—"; computing
        // `ratio = current / baseline = 0.0×` would falsely imply a
        // faster-than-baseline reading.
        let _g = GUARD.lock().unwrap();
        clear();
        let (_tx, rx) = tokio::sync::watch::channel(ControllerSnapshot {
            cwnd: 5,
            baseline_latency: std::time::Duration::from_millis(2),
            current_latency: std::time::Duration::ZERO,
            samples_seen: 42,
        });
        register_unit("idle-short-window", rx);
        let out = render_lines();
        assert!(out.contains("ratio="));
        assert!(
            out.contains("—"),
            "expected '—' for unset current, got {out:?}"
        );
        assert!(
            !out.contains("ratio=   0.0×"),
            "ratio must not render as 0.0× when current is unset: {out}",
        );
        clear();
    }

    #[test]
    fn render_lines_columns_are_aligned_across_rows() {
        // Regression: durations like "58ns" (4 chars) and "33.5µs" (6
        // chars) used to land in unpadded columns, so consecutive rows
        // were visually misaligned. With FIELD_WIDTH right-alignment,
        // each "key=" anchor must start at the same display-column on
        // every rendered row. We compare char counts rather than byte
        // offsets so the multi-byte `µ` doesn't confuse the check.
        let _g = GUARD.lock().unwrap();
        clear();
        let (_tx_a, rx_a) = tokio::sync::watch::channel(ControllerSnapshot {
            cwnd: 1,
            baseline_latency: std::time::Duration::from_nanos(58),
            current_latency: std::time::Duration::from_micros(33),
            samples_seen: 629_000,
        });
        let (_tx_b, rx_b) = tokio::sync::watch::channel(ControllerSnapshot {
            cwnd: 1,
            baseline_latency: std::time::Duration::from_micros(1700),
            current_latency: std::time::Duration::from_micros(3500),
            samples_seen: 64_600,
        });
        register_unit("walk-src", rx_a);
        register_unit("meta-src", rx_b);
        let out = render_lines();
        let row_lines: Vec<&str> = out
            .split('\n')
            .filter(|s| !s.is_empty() && *s != SEPARATOR)
            .collect();
        assert_eq!(row_lines.len(), 2);
        let char_offset = |row: &str, key: &str| -> Option<usize> {
            let byte = row.find(key)?;
            Some(row[..byte].chars().count())
        };
        for key in ["cwnd=", "base=", "curr=", "ratio=", "samples="] {
            let col_a = char_offset(row_lines[0], key);
            let col_b = char_offset(row_lines[1], key);
            assert_eq!(col_a, col_b, "{key} column misaligned: {row_lines:?}");
            assert!(col_a.is_some(), "{key} missing from row: {row_lines:?}");
        }
        clear();
    }
}
