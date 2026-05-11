//! Pure rendering of per-(side, op) latency distribution panels.
//!
//! The panel sits below the existing [`crate::observability::render_lines`]
//! summary block and visualizes each active controller's most recent
//! snapshot as a small ASCII histogram. Truncated to the densest 8 bands
//! to fit beside the existing one-line-per-controller summary without
//! overwhelming the terminal.

use hdrhistogram::Histogram;

/// One unit's data for the panel: its label, the snapshot, and the
/// snapshot's covered duration (so the header line can name the
/// window).
pub struct PanelUnit<'a> {
    pub label: &'a str,
    pub histogram: &'a Histogram<u64>,
    pub interval: std::time::Duration,
}

/// Number of histogram bands rendered per unit. Chosen to fit on screen
/// alongside the existing one-line summary without overwhelming the
/// progress display; smaller than the natural HDR bucket count.
const ROWS_PER_UNIT: usize = 8;
/// Max characters for the bar of the densest bucket.
const BAR_WIDTH: usize = 24;

const SEPARATOR: &str = "-----------------------";

/// Render the distribution panel for the given units. Empty units (no
/// samples) are skipped. Returns an empty string if every unit is
/// empty.
#[must_use]
pub fn render_histogram_panel(units: &[PanelUnit]) -> String {
    let visible: Vec<_> = units.iter().filter(|u| !u.histogram.is_empty()).collect();
    if visible.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    out.push('\n');
    out.push_str(SEPARATOR);
    for unit in &visible {
        out.push('\n');
        render_unit(&mut out, unit);
    }
    out
}

fn render_unit(out: &mut String, unit: &PanelUnit) {
    let n = unit.histogram.len();
    out.push_str(&format!(
        "{label} distribution (last {secs:.1}s, n={n}):\n",
        label = unit.label,
        secs = unit.interval.as_secs_f64(),
        n = n,
    ));
    // HDR's iter_log returns values bucketed at log-scale boundaries.
    // We iterate, find the densest 8 contiguous bands centered on the
    // mode, and render those.
    let bands: Vec<(u64, u64)> = unit
        .histogram
        .iter_log(1, 2.0)
        .map(|v| (v.value_iterated_to(), v.count_since_last_iteration()))
        .filter(|&(_, c)| c > 0)
        .collect();
    if bands.is_empty() {
        out.push_str("  (no samples)\n");
        return;
    }
    let mode_idx = bands
        .iter()
        .enumerate()
        .max_by_key(|(_, (_, c))| *c)
        .map(|(i, _)| i)
        .unwrap_or(0);
    let half = ROWS_PER_UNIT / 2;
    let lo = mode_idx.saturating_sub(half);
    let hi = (lo + ROWS_PER_UNIT).min(bands.len());
    let lo = hi.saturating_sub(ROWS_PER_UNIT);
    let visible_bands = &bands[lo..hi];
    let max_count = visible_bands.iter().map(|&(_, c)| c).max().unwrap_or(1);
    for &(value, count) in visible_bands {
        let bar_len = ((count * BAR_WIDTH as u64) / max_count) as usize;
        let bar = "█".repeat(bar_len);
        out.push_str(&format!(
            "  {value:>8} {bar:<bar_width$} {count}\n",
            value = format_micros(value),
            bar = bar,
            bar_width = BAR_WIDTH,
            count = count,
        ));
    }
}

fn format_micros(v: u64) -> String {
    if v < 1_000 {
        format!("{v}µs")
    } else if v < 1_000_000 {
        format!("{:.1}ms", v as f64 / 1_000.0)
    } else {
        format!("{:.1}s", v as f64 / 1_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hist(samples: &[u64]) -> Histogram<u64> {
        let mut h = Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).unwrap();
        for &v in samples {
            h.record(v).unwrap();
        }
        h
    }

    #[test]
    fn empty_units_produce_empty_output() {
        let h = make_hist(&[]);
        let units = [PanelUnit {
            label: "src-stat",
            histogram: &h,
            interval: std::time::Duration::from_secs(1),
        }];
        assert_eq!(render_histogram_panel(&units), "");
    }

    #[test]
    fn renders_per_unit_header_and_bands() {
        // 100 samples clustered around 100µs / 200µs; render must include
        // the unit's label, the "last 1.0s, n=100" header, and at least
        // one bar character.
        let mut samples = vec![100u64; 70];
        samples.extend(vec![200u64; 30]);
        let h = make_hist(&samples);
        let units = [PanelUnit {
            label: "src-stat",
            histogram: &h,
            interval: std::time::Duration::from_secs(1),
        }];
        let out = render_histogram_panel(&units);
        assert!(out.contains("src-stat distribution"), "got: {out}");
        assert!(out.contains("n=100"), "got: {out}");
        assert!(
            out.contains('█'),
            "expected at least one bar block, got: {out}"
        );
    }

    #[test]
    fn empty_unit_is_skipped_among_active_units() {
        let h_empty = make_hist(&[]);
        let h_full = make_hist(&[100, 200, 300]);
        let units = [
            PanelUnit {
                label: "idle",
                histogram: &h_empty,
                interval: std::time::Duration::from_secs(1),
            },
            PanelUnit {
                label: "active",
                histogram: &h_full,
                interval: std::time::Duration::from_secs(1),
            },
        ];
        let out = render_histogram_panel(&units);
        assert!(!out.contains("idle"), "idle unit must be hidden: {out}");
        assert!(out.contains("active"), "active unit must show: {out}");
    }
}
