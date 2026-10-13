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

From the repository root, use the locked Nix reporting environment to reproduce the analysis and
figures:

```bash
nix develop .#reports --command python3 analysis/2026-10-11/analyze.py
```

The reference environment uses Python 3.14.7, NumPy 2.5.2, and Matplotlib 3.11.1. Without Nix,
install the pinned [Python dependencies](requirements.txt) into a Python environment, then run:

```bash
python3 -m pip install -r analysis/2026-10-11/requirements.txt
python3 analysis/2026-10-11/analyze.py
```

The Python pins alone do not guarantee byte-identical SVGs across platforms: fonts and native
libraries can affect rendering. Use the locked Nix environment for reference regeneration. Python
assertions must remain enabled; the script rejects `-O`, `-OO`, and `PYTHONOPTIMIZE` modes that
disable them.

This verifies all packet checksums, embedded command/timing log hashes, and recorded summary
statistics before writing `analysis.json` and the ten SVG figures used by the report under
`docs/reports/figures/performance/`. Copy, destination-sync, and validation measurements remain in
the JSON and raw packet. The audit's `archive_sha256` is null for directory input: the original
archive hash is retained in `packet-origin.json`, but cannot be recomputed from unpacked files.
`packet_checksums_sha256` identifies the same checksum manifest for both directory and archive
inputs.

The script also accepts the original `.tar.gz` archive. Use `--output` and `--figures-output` to put
derived outputs elsewhere. `--png-output /tmp/rcp-figures` optionally exports PNG copies; SVG is the
only figure format kept in the repository.

Run the audit and figure regression tests with:

```bash
nix develop .#reports --command just analysis-test
```
