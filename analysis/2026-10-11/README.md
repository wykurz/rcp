# Performance report measurement evidence

The [performance report](../../docs/reports/performance.md) uses this data. All 65 files from the
supplied clean measurement packet are preserved byte for byte in [raw/](raw/), including its
original checksum manifest. The packet is approximately 30.4 MB of UTF-8 text: JSON, JSONL, logs,
scripts, and provenance documentation. It does not contain the generated file payloads being copied.

- [Packet identity](packet-origin.json): original archive filename and SHA-256, manifest SHA-256,
  file count, and byte count.
- [Original checksums](raw/SHA256SUMS): hashes of the other 64 files in the packet.
- [Source inventory](raw/SOURCES.json): source campaigns and measurement counts.
- [Normalized analysis](analysis.json): per-repetition measurements, selected and superseded
  configurations, command examples, computed statistics, coverage, and comparison ratios.
- [Analysis script](analyze.py): audits the packet, recomputes the figures, and preserves the agreed
  selection rules. Original JSON documents retain the complete records and embedded logs beyond the
  fields used in the normalized analysis.

The raw files are evidence, including their historical labels and notes. The report explains the
interpretation and selection agreed with the experiment owner. Do not format or edit the raw files.

Verify the original packet hashes:

```bash
cd analysis/2026-10-11/raw
sha256sum --check SHA256SUMS
```

From the repository root, with NumPy and Matplotlib installed:

```bash
python3 analysis/2026-10-11/analyze.py
```

This verifies all packet checksums, embedded command/timing log hashes, and recorded summary
statistics before writing `analysis.json` and SVG figures under `docs/reports/figures/performance/`.
The audit's `archive_sha256` is null for directory input: the original archive hash is retained in
`packet-origin.json`, but cannot be recomputed from unpacked files. `packet_checksums_sha256`
identifies the same checksum manifest for both directory and archive inputs.

The script also accepts the original `.tar.gz` archive. Use `--output` and `--figures-output` to put
derived outputs elsewhere. `--png-output /tmp/rcp-figures` optionally exports PNG copies; SVG is the
only figure format kept in the repository.
