# Remote teardown trace conformance

When a destination reads a short body for file A while the source is waiting for file B's
`DirectoryReady`, the destination must close control without `DestinationDone`. Both endpoints must
return errors.

```text
DataPrefix ─┐                     ┌─ DestinationError (short body)
            ├─ ShortData ─────────┤
WaitB ──────┘                     └─ ControlEOF ── SourceError
```

The Rust tests (`remote_teardown_trace` over loopback and `remote_teardown_trace_over_turmoil` over
Turmoil's deterministic network) run the real source discovery/sender and destination receiver. A
gated relay holds B's Ready frame, accepts the complete A payload from the source, and forwards only
the prefix to the destination. Its gates use observed events, not sleeps. It records EOF on the
destination-facing socket before forwarding EOF to the source. The endpoint results supply the two
error events; the tests check their causes, validate the causal order, and reject any Done.

The TLA+ model describes this bounded observation, not the full protocol. It permits either order
between the two branches of the diagram. TLC checks whether every recorded step is permitted and
whether the trace reaches both terminal results. The model's liveness check assumes enabled
transitions are eventually taken; the Rust tests have their own deadlines. This is conformance for
the observed runs, not a proof about unobserved interleavings, TLS, or SSH.

## Run

The Rust tests are included in normal tests. TLC is opt-in and needs Java and the
[official TLC 1.7.4 jar](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.4); it is not a
runtime or CI dependency.

```sh
work=$(mktemp -d)
RCP_TEARDOWN_TRACE="$work/trace" just test -p rcp-tools-rcp --lib -E 'test(=source::discovery::tests::trace_teardown::remote_teardown_trace)'
python3 rcp/spec/remote-teardown/check.py /path/to/tla2tools.jar "$work/trace"
RCP_TURMOIL_TEARDOWN_TRACE="$work/turmoil-trace" just test -p rcp-tools-rcp --lib -E 'test(=source::discovery::tests::trace_teardown::remote_teardown_trace_over_turmoil)'
python3 rcp/spec/remote-teardown/check.py /path/to/tla2tools.jar "$work/turmoil-trace"
rm -r -- "$work"
```
