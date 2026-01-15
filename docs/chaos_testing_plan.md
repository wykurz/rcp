# Chaos Testing Implementation Plan

This document tracks the implementation of chaos and failure injection testing for rcp.

## Goals

- Improve rcp's resilience to adverse conditions
- Test error handling and recovery paths
- Verify graceful degradation under stress
- Ensure good user experience when things go wrong
- All tests must be fully reproducible in CI (GitHub Actions)

## Constraints

- GitHub Actions has limited resources (CPU, memory, time)
- Tests must complete in reasonable time
- Avoid resource-intensive stress tests that may timeout or flake

---

## Phase 1: Network Condition Simulation

**Status**: Complete

Use Linux `tc` (traffic control) in Docker containers to simulate adverse network conditions.

### Tasks

- [x] Update Dockerfile to include `iproute2` package
- [x] Add helper functions to `docker_env.rs` for network manipulation:
  - [x] `add_latency(container, delay_ms, jitter_ms)` - add fixed latency with optional jitter
  - [x] `add_packet_loss(container, percent)` - drop packets randomly
  - [x] `add_bandwidth_limit(container, rate_kbit)` - throttle bandwidth
  - [x] `add_network_conditions(container, delay_ms, loss_percent)` - combined conditions
  - [x] `clear_network_conditions(container)` - reset to normal
- [x] Add `CAP_NET_ADMIN` capability to docker-compose containers
- [x] Write tests for:
  - [x] High latency (200ms) - verify copy succeeds and takes longer
  - [ ] ~~Packet loss~~ - disabled (see note below)
  - [x] Low bandwidth (1 Mbit/s) - verify transfer completes (slowly)
  - [ ] ~~Combined conditions~~ - disabled (see note below)
  - [x] Directory copy under latency - verify protocol handles multiple RTTs

**Note on packet loss tests**: Packet loss simulation via `tc netem loss` affects ALL
traffic on the interface, including the SSH session used by rcp to spawn rcpd. This
causes SSH to hang before the copy even starts. A future improvement would use iptables
rules targeting specific ports, or apply tc rules after SSH is established.

### Implementation Notes

Network conditions via `tc`:
```bash
# Add 200ms latency
tc qdisc add dev eth0 root netem delay 200ms

# Add 5% packet loss
tc qdisc add dev eth0 root netem loss 5%

# Limit bandwidth to 1mbit
tc qdisc add dev eth0 root tbf rate 1mbit burst 32kbit latency 400ms

# Clear all rules
tc qdisc del dev eth0 root
```

### Success Criteria

- Tests pass reliably in CI
- Network conditions are applied and cleared correctly
- Error messages are clear when operations fail due to network issues

---

## Phase 2: Process Chaos (Kill/Pause)

**Status**: Complete

Test rcp's behavior when rcpd processes die or hang unexpectedly.

### Tasks

- [x] Add helper functions to `docker_env.rs`:
  - [x] `kill_rcpd(container)` - kill rcpd process by name
  - [x] `pause_rcpd(container)` - SIGSTOP the process
  - [x] `resume_rcpd(container)` - SIGCONT the process
  - [x] `is_rcpd_running(container)` - check if rcpd is running
  - [x] `get_rcpd_pids(container)` - get PIDs of rcpd processes
  - [x] `spawn_rcp(args)` - spawn rcp in background for async testing
- [x] Write tests for:
  - [x] Kill rcpd early (before connections established) - tests "connection refused" path
  - [x] Kill rcpd mid-transfer (after connections established) - tests TCP failure detection
  - [x] Pause rcpd (simulates hang) - verify timeout behavior (~15s with default timeout)
  - [x] Master (rcp) killed - verify rcpd cleanup via stdin watchdog
  - [x] Process helpers meta-test

### Implementation Notes

The existing `exec_rcp_with_delayed_rcpd` pattern can be extended. We can:
1. Start a transfer in background
2. Wait for specific stage (via log output or timing)
3. Kill/pause the target process
4. Verify cleanup and error handling

### Success Criteria

- No orphaned rcpd processes after any failure scenario
- Clear error messages indicating what failed
- Partial transfers are handled gracefully (no corruption)

---

## Phase 3: I/O Error Simulation

**Status**: Complete

Test behavior when filesystem operations fail.

### Tasks

- [x] Add helper functions to `docker_env.rs`:
  - [x] `mount_tmpfs(container, path, size_kb)` - mount small tmpfs for ENOSPC testing
  - [x] `unmount_tmpfs(container, path)` - cleanup tmpfs mounts
  - [x] `available_space(container, path)` - check available disk space
  - [x] `chmod(container, path, mode)` - change file permissions
  - [x] `chown(container, path, owner)` - change file ownership
- [x] Create test scenarios for:
  - [x] Disk full (ENOSPC) - use small tmpfs, verify "No space left on device" in error
  - [x] Permission denied on destination directory
  - [x] Permission denied on source file (no read permission)
  - [ ] ~~Read errors on source~~ - deferred (requires special device files)
- [x] Verify error chain preservation (root cause visible in stderr)
- [ ] ~~Test `--fail-early` vs continue behavior~~ - covered by existing tests

### Implementation Notes

Disk full is simulated by:
1. Mounting a small tmpfs (512KB) on destination
2. Creating a source file larger than tmpfs (1MB)
3. Attempting copy, verifying ENOSPC error is reported

Permission errors are tested by:
1. Removing write permission from destination directory (chmod 555)
2. Removing read permission from source file (chmod 000)
3. Verifying "Permission denied" appears in error output

### Success Criteria

- [x] ENOSPC errors are reported clearly
- [x] Error chain shows "No space left on device"
- [x] Permission denied errors show root cause

---

## Phase 4: Protocol Edge Cases (Future)

**Status**: Not started / Lower priority

Test protocol robustness with unusual message patterns.

### Potential Tasks

- [ ] Connection drops at specific protocol stages
- [ ] Very slow sender (backpressure testing)
- [ ] Maximum message sizes
- [ ] Many concurrent connections hitting limits

### Notes

This phase may require test hooks in the protocol layer or mock transports.
Lower priority than phases 1-3 which test real-world failure modes.

---

## CI Integration

### Current Docker Test Job

The existing `.github/workflows/validate.yml` Docker job can be extended:

```yaml
docker-chaos-tests:
  # Run after regular docker tests pass
  needs: docker-tests
  # ... setup steps ...
  - name: Run chaos tests
    run: cargo nextest run --profile docker --run-ignored only -E 'test(~chaos)'
```

### Test Naming Convention

Chaos tests should follow naming pattern: `test_chaos_*` or include `chaos` in name.
This allows running them separately: `cargo nextest run -E 'test(~chaos)'`

---

## Progress Log

| Date | Phase | Change | Commit |
|------|-------|--------|--------|
| 2026-01-13 | 1 | Add iproute2 to Dockerfile, CAP_NET_ADMIN to docker-compose | - |
| 2026-01-13 | 1 | Add network simulation helpers to docker_env.rs | - |
| 2026-01-13 | 1 | Add chaos network tests (docker_chaos_network.rs) | - |
| 2026-01-14 | 2 | Add process chaos helpers (kill/pause/resume rcpd) | - |
| 2026-01-14 | 2 | Add chaos process tests (docker_chaos_process.rs) | - |
| 2026-01-14 | 3 | Add I/O chaos helpers (tmpfs mount, chmod, chown) | - |
| 2026-01-14 | 3 | Add chaos I/O tests (docker_chaos_io.rs) | - |

---

## References

- `docs/testing.md` - Overall testing documentation
- `docs/remote_protocol.md` - Protocol design (for understanding failure points)
- `rcp/tests/support/docker_env.rs` - Docker test helpers (network, process, I/O chaos)
- `rcp/tests/docker_multi_host*.rs` - Existing Docker tests
- `rcp/tests/docker_chaos_network.rs` - Network chaos tests (Phase 1)
- `rcp/tests/docker_chaos_process.rs` - Process chaos tests (Phase 2)
- `rcp/tests/docker_chaos_io.rs` - I/O chaos tests (Phase 3)
