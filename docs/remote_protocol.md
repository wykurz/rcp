# Remote Copy Protocol Design Document

This is the detailed implementation reference. Start with the
[remote component design](../remote/DESIGN.md) for the ownership and source map, or
[remote copy operations](remote_copy.md) for user-facing setup and troubleshooting. Test and
measurement procedures belong in [testing](testing.md) and [benchmarking](benchmarking.md).

## 1. Architecture Overview

### 1.1 Three-Component Architecture

The remote copy system consists of three distinct components:

1. **Master (rcp)**: Coordinates the entire operation, runs on the client machine where the user
   invokes `rcp`
2. **Source (rcpd)**: Runs on the source host, responsible for reading and sending files
3. **Destination (rcpd)**: Runs on the destination host, responsible for receiving and writing files

### 1.2 Component Spawning and Lifecycle

**Spawning Sequence:**

1. User invokes `rcp user@host1:/src user@host2:/dst`
2. Master validates pure configuration, then prepares compatible source and destination rcpd
   endpoints. Equal `SshSession` values share one preparation; distinct endpoints may prepare
   concurrently.
3. Master spawns source rcpd via SSH: `ssh user@host1 rcpd --role=source ...`
4. Source emits `RCP_READY <hex_fingerprint|-> <F> <E>` as the first stderr record (`-` when
   `--no-encryption` disables source↔destination TLS) and switches its SSH `stdin`/`stdout` pipes to
   binary framed protocol I/O (redirecting process fd 1 to `stderr` so stray writes cannot corrupt
   framing). The master immediately starts the source stdout demux and stdin heartbeat tasks.
5. Master constructs the destination configuration from source readiness, spawns destination rcpd,
   verifies its readiness reports the same `F/E` and encryption mode, and starts the destination
   stdout demux and stdin heartbeat tasks.

**Propagated security flags:** When the master runs with `--require-toctou-safe`, it mirrors the
flag into each rcpd's spawn arguments (via `RcpdConfig::to_args()`). Each rcpd then arms strict
operand resolution before any filesystem work: its operand root/parent opens resolve with
`openat2(RESOLVE_NO_SYMLINKS)`, and it refuses to run on kernels without `openat2` (Linux 5.6+) —
the refusal is printed as a single line on stderr, where the master's handshake reader surfaces it.
The master lints the operands' strict form (absolute as written, lexically normal; `~`-relative
forms are rejected) before spawning. Note the asymmetry: the source's `MasterHello::Source` carries
both `src` and `dst`, so the source `rcpd` opens+validates its source parent up front, but
`MasterHello::Destination` carries **no path** — the destination `rcpd` learns the destination only
from the source's per-entry messages, so it validates its prefix only when it actually opens the
destination to write (a `--dry-run` or fully-filtered source writes nothing and therefore does not
separately validate it; see the strict-operand residual in [tocttou.md](tocttou.md)).
Version-matched rcpd binaries (see binary discovery in [remote_copy.md](remote_copy.md)) understand
the spawn argument.

**Source-owned file-work ceiling:** Let `F` be the logical file ceiling, `M` the configured
`--max-connections`, and `E = min(F, M)`. When `--max-files-in-flight` is omitted, the source `rcpd`
selects `F = max(std::thread::available_parallelism(), 4)` on the source host; the source readiness
record makes that decision authoritative for the destination. An explicit `F` remains
master-authoritative and becomes `--max-files-in-flight=N|unlimited` on both roles. Finite and
unlimited legacy input use a hidden typed forwarding argument, never the deprecated spelling; this
preserves `--max-open-files` provenance for clamp notices without repeating its deprecation warning.
Explicit or legacy unlimited yields `E = M`. The automatic destination uses a hidden
resolved-automatic argument that preserves automatic provenance while carrying source `F`.

The resolved `E` and pending capacity `P = E × pending-writes-multiplier` use checked arithmetic,
must be nonzero, and must not exceed `tokio::sync::Semaphore::MAX_PERMITS`. Explicit capacity can be
validated before remote-home expansion or SSH. For automatic capacity, the master validates the
configured connection upper bound before remote side effects; the source resolves and validates the
actual CPU-selected capacity before it announces readiness and before destination spawn. Wire
revision 14 adds SSH data/control transport, direct-TCP fallback, load-balanced peer verification,
and established-failure tracking (`PeerIdentity`, `DestinationTransport` including 3-host
`source_peer` / `source_node_endpoint`, `MasterHello::Source::accept_timeout_sec`,
`MasterHello::Destination::source_data_addrs`,
`SourceMasterHello::{tunnel_target_ip, peer_identity, ssh_node_endpoint}`,
`SourceMessage::DryRunReady`, `MasterToRcpd::SshFallbackEndpoints`,
`RcpdToMaster::DirectConnectFailed`, and `RcpdResult::Failure::peer_connected`), building on
revision 13's SSH `stdin`/`stdout` master↔rcpd transport (`RCP_READY`, `MasterToRcpd`,
`RcpdToMaster`), revision 12's process statistics counters, and revision 11's receiver copy-buffer
retention configuration. It requires exact-version rcp/rcpd binaries.

Each daemon attempts to raise its soft descriptor limit to the inherited hard limit before runtime
and admission setup (§7.8). For normal copies, it installs one joint leaf/directory resource plan
before readiness. Insufficient known descriptor headroom produces a typed `RCP_ERROR` startup
refusal with the effective limit, stream count, and remedies.

For dry runs, the master passes hidden `--preview-only` to both roles. Before readiness, each daemon
selects local leaf/metadata admission without reserving E data streams or N/R directory lifetimes.
Both roles validate that `MasterHello` carries the same preview policy before dispatching work.

A remote preview reports traversal failures as an unsuccessful operation, including descendant
metadata, symlink, directory-listing, and unsupported-entry errors. Without `--fail-early`, the
source continues previewing siblings, completes the empty `DiscoveryComplete`/`DestinationDone`
exchange, and returns the collected error with its partial preview summary. With `--fail-early`, it
stops at the first error. Explicitly excluded or permitted `--skip-specials` entries are not errors.
A preview does not create its destination or verify that file contents can be read.

**Special Case - Same Host Copies:** When source and destination are on the same host, the master:

- Discovers, verifies, and deploys rcpd only once (if needed)
- Starts two separate rcpd processes with different roles
- Both processes share the same SSH session but have distinct connections

Sharing an SSH session does not enable the local overlap checks: remote source and destination trees
must be disjoint (see [remote overlap limits](tocttou.md#remote-sourcedestination-overlap)).

**Auto-deployment compatibility:** Auto-deployment applies the same exact compatibility policy as
normal remote discovery at both boundaries. The master runs `--protocol-version` on each local
candidate in search order (beside `rcp`, then the local shell's POSIX `command -v rcpd`) and
continues to later candidates when one is stale, stalls its two-second version probe, or is
otherwise unusable. An ordinary local PATH miss is retained in the final searched-candidates
diagnostic. A timed-out local candidate has its pipes released and receives a kill request; the
invocation cleanup supervisor retains child ownership and reaps it while the search continues. The
selected local deployment payload is read on a cleanup-owned disposable OS worker under the same
configured bootstrap deadline and peer cancellation. A blocked filesystem syscall can finish later
on that worker, but it cannot retain Tokio runtime or remote-resource ownership. Remote discovery
uses the remote shell's `command -v rcpd` rather than an external `which`. Remote version probes,
including post-deployment verification, use the initiating `rcp` process's
`--remote-copy-conn-timeout-sec` deadline so slow-but-healthy hosts can be given an appropriate
budget without allowing a hanging SSH channel to block fallback indefinitely. The master names the
cache target with the accepted version's compatibility tag, transfers and publishes the binary, then
probes that deployed remote path before constructing either role's spawn command. Thus neither
co-location nor a current-looking cache filename is treated as proof that the binary implements the
current serialized and rcpd spawn contract. For distinct hosts, the first preparation failure
cooperatively cancels its peer without dropping owned work or replacing the original error. SSH
control-socket readiness uses the same configured deadline and one cancellation-aware filesystem
worker for its whole polling lifetime. Every read-only remote bootstrap command — HOME lookup,
executable checks, PATH discovery, and version probes — goes through one helper that requires that
per-stage timeout; expiry aborts and joins its local SSH-channel task. Remote cache cleanup is best
effort and uses the same bounded helper. Binary deployment uses the timeout for its HOME lookup, SSH
command, readiness marker, and each payload-write idle period, but not as a wall-clock limit on
transmitting the binary. Post-EOF checksum verification and publication use a bounded stage of at
least 60 seconds. On peer cancellation, the transfer gets a bounded grace to close stdin and finish
before its local SSH-channel task is aborted and joined.

The SSH multiplex master runs as a retained foreground `ssh -M -N` process spawned on the dedicated
`rcp-cleanup-supervisor` OS thread; on Linux, `PR_SET_PDEATHSIG(SIGTERM)` (with a post-fork
`getppid()` check) binds every `ssh -M -N` child to the owning process so an abrupt exit (`SIGKILL`
or OOM killer) terminates destination-owned and master-owned tunnel processes and their listeners
promptly. Explicit `ForkAfterAuthentication=no`, `ControlPersist=no`, and `ClearAllForwardings=yes`
command-line overrides prevent user or system SSH configuration from backgrounding it or colliding
with user-configured port forwards, and `ServerAliveInterval` / `ServerAliveCountMax` (or
`ServerAliveInterval=0` when `--remote-keepalive-sec=0`) enforce the configured keepalive budget on
the SSH transport. For 3-host SSH copies where agent forwarding is active or configured
(`--ssh-forward-agent` or `ForwardAgent` in `~/.ssh/config`), the master authenticates the
destination's foreground `ssh -M -N` session up front under the user's configured
`StrictHostKeyChecking` policy (omitting `StrictHostKeyChecking=no`) **before** any source SSH
connection can update `known_hosts` (including across distinct routes that share an OpenSSH
`HostKeyAlias` or when a source alias permits `accept-new`), and retains that single verified master
across optional destination `~` HOME expansion (over the native mux with agent forwarding off) and
destination `rcpd` setup. The destination daemon is then spawned through
`ssh -E <private_exec_log> -S <control_path> -T -o ForkAfterAuthentication=no` so OpenSSH propagates
`SSH_AUTH_SOCK` into the multiplexed session without polluting `RCP_READY` on stderr with local
OpenSSH diagnostics (the private log tail is read with a bounded wait on a disposable filesystem
worker on startup or nonzero-exit failures, while supervised control-directory cleanup owns its
removal after the master exits); `--ssh-forward-agent` passes `-o ForwardAgent=yes` only on the
destination endpoint. Cancelling setup aborts that owner and its single-owner preparation guard
terminates and reaps the connecting process before removing the control directory. The configured
SSH executable is resolved inside a known-local shell child. Brackets that delimit IPv6 in an rcp
operand are removed from the direct OpenSSH host argument; leaving them in argv makes OpenSSH
resolve the brackets as literal hostname characters. After setup, commands open channels with the
native OpenSSH multiplex protocol over the retained control socket rather than synchronously
spawning another local `ssh`, so their configured deadline also covers exec-channel creation. A
preparation guard owns the foreground child and private control directory together until success
transfers both to a cloneable managed session through prepared and running daemon states. Whichever
owner exits signals the master before returning. The cleanup supervisor is created before any remote
resource is accepted. Process reaping is queued there, so it cannot block or depend on a Tokio
runtime that may already be shutting down. The supervisor normally dispatches blocking cleanup to a
worker; worker-creation failure runs the job on the supervisor rather than the original
resource-owning submitter. A failed supervisor channel tries an isolated worker and, if no worker
can be created, leaks the job so the OS reclaims its owned resources at process exit. Every reaper
for an SSH master or an interrupted local candidate must receive the cleanup scope's typed budget;
its effective deadline is the earlier of its per-job deadline and the invocation's shared final
deadline. The common poll helper therefore cannot spin forever on a child that never becomes
reapable. The control directory is removed only after the retained master is confirmed exited;
expiry preserves it rather than deleting the socket from under a possibly-live master. The master
threads actual local operand roots through remote-HOME lookup and endpoint preparation; canonical
control-directory candidates inside those trees are rejected, while remote-to-remote copies do not
infer an exclusion from the working directory. The filesystem root cannot exclude candidates because
every absolute socket path lies beneath it. Nested cleanup runs inside its parent cleanup worker,
and separate invocations cannot drain one another's workers. Before process exit, the CLI gives the
scope one bounded budget to wait for its last resource owner, supervisor, and every cleanup job;
work still blocked after that grace is abandoned with the process. Daemon waits run concurrently
across endpoints while tracing receivers stay live, then any remaining receiver tasks share one
final drain deadline.

Normal daemon discovery does not require `HOME`: when it is absent, the deployed-cache candidate is
skipped and same-directory/PATH discovery continues. Remote `~` expansion and deployment into the
cache do require a usable `HOME`, so those operations fail with a targeted diagnostic instead of
constructing a path under `/`. Expansion recognizes only a leading `~` by itself or followed by `/`.
Repeated separators immediately after it remain home-relative, while subsequent `~` path components
are literal: `host:~//docs` resolves to `HOME/docs`, and `host:~/~/file` to `HOME/~/file`. Local
quoted operands and the `localhost:` escape hatch use the same suffix rules with the local `HOME`.
Expansion does not canonicalize `.` or `..` components.

Deployment stages, verifies, and publishes through one remote `sh` transaction. Before anything can
create the unique temp path, that shell installs an `EXIT` trap which removes it. After directory
creation and opening the staging file on descriptor 3, it emits `RCP_DEPLOY_READY` on stdout. The
master bounds and validates that marker before sending payload bytes; bounded stdout preamble and
stderr are retained when setup fails. Checksum mismatch, transfer failure, SSH-channel disconnect,
and handled `HUP`/`INT`/`TERM` therefore all clean up in the same process that owns the writer. Only
a checksum-verified file reaches the final same-directory rename. Cancellation closes staging stdin
and waits briefly for the command while its owner drains both pipes; after that grace the local
SSH-channel task is aborted and joined. The configured timeout limits idle payload writes, not total
binary-transmission duration. The remote shell retains durable cleanup ownership independently of
the local Tokio task. A temp name surviving an unhandled remote termination is private to that
deployment and is never discovered, executed, or adopted by a retry.

**Startup stderr ownership and notices:** A successfully started daemon reserves its first stderr
line for exactly one readiness record. The master bounds both SSH exec-channel creation and that
read by `--remote-copy-conn-timeout-sec`, and rejects a record larger than 64 KiB. Chrome-trace,
flamegraph, Tokio-console, legacy-option, and explicit concurrency-clamp announcements are collected
until tracing is installed and emitted through the default-visible `rcp::notice` target. Remote
tracing queues daemon notices until the daemon's SSH stdout writer starts right after `RCP_READY`,
so they reach master output without becoming readiness preamble. An intentional fatal startup
refusal instead emits one `RCP_ERROR <diagnostic>` record and exits, including configuration
failures found before tracing is installed. The master treats that typed record as a nested failure
cause, closes its stdin, and gives a directly owned reaper a bounded grace before returning. The SSH
child and both output drains remain lexically inside that timeout future, so grace expiry or caller
cancellation drops them and releases the managed session instead of detaching ownership. If startup
fails without a typed record, captured stdout and remaining stderr are attached to the handshake
error. Arbitrary stderr remains an invalid readiness record. After readiness, `stdout` carries the
framed binary `RcpdToMaster` stream while the stderr collector forwards raw daemon stderr at debug
verbosity as it arrives and retains only a bounded tail. It is joined on daemon completion, so a
nonzero exit keeps stderr diagnostics without allowing unbounded output to grow in memory or leaving
a collector task detached. An explicit `F` reduced by `M`, an automatic `F` reduced by an explicit
`M`, an explicit `M` reduced by the source's `F`, or an explicit limit reduced by endpoint
descriptor safety produces a notice naming the requested and effective values. The ordinary
automatic/default intersection remains quiet. The master retains the source process and starts its
stdout demux immediately after readiness, before destination configuration or startup. Every later
startup and protocol exit closes daemon stdin, waits for owned daemon processes, and gives stdout
demux tasks a bounded drain, so already-queued source notices remain visible alongside a destination
failure rather than being dropped during unwinding.

### 1.3 Connection Topology

The system uses a **triangle topology** combining SSH exec stdio (Master ↔ daemons) with direct TCP
connections (Source ↔ Destination):

![Triangle topology: master, source, destination](assets/remote_architecture.svg)

<details>
<summary>Text transcript</summary>

The diagram distinguishes the Master ↔ rcpd SSH exec channels from the Source ↔ Destination TCP
connections and shows every application-data direction:

1. Master uses SSH to start source rcpd. It reports its optional TLS fingerprint (for
   source↔destination mTLS), logical file ceiling `F`, and effective stream count `E` back to master
   in `RCP_READY` on SSH stderr.
2. Master uses the source SSH exec channel's `stdin` (`MasterToRcpd`: `MasterHello` and periodic
   `Heartbeat`) and `stdout` (`RcpdToMaster`: `SourceHello`, `RcpdResult`, and `Tracing`) for
   control and tracing/progress.
3. Master starts destination rcpd with source `F/E`, verifies its readiness matches, and uses the
   destination SSH exec channel's `stdin`/`stdout` in the same way.
4. Master sends `MasterHello::Source`; source returns `SourceMasterHello` and later `RcpdResult`.
5. Master sends `MasterHello::Destination`; destination later returns `RcpdResult`.
6. Destination opens the source/destination bidirectional TCP control connection. Directory and
   symlink messages flow source → destination; manifests, directory acknowledgements, and
   `DestinationDone` flow destination → source.
7. For copy operations, destination also opens the pooled TCP data connections to source. File
   headers and bytes flow only source → destination and never through master. A dry run uses only
   control connections and starts no destination data workers.

By default, every Source ↔ Destination TCP connection uses TLS 1.3 and mutual
certificate-fingerprint authentication, while Master ↔ rcpd traffic is protected by SSH. The
`--no-encryption` option disables TLS and certificate authentication on Source ↔ Destination TCP
connections; Master ↔ rcpd control and tracing remain protected by SSH.

</details>

**Connection Details:**

1. **Master ↔ Source**: SSH exec channel `stdin`/`stdout` (length-delimited bitcode framing):
   - **Master → Source (`stdin`)**: `MasterToRcpd::Hello` and periodic `MasterToRcpd::Heartbeat`
   - **Source → Master (`stdout`)**: Multiplexed `RcpdToMaster::{SourceHello, Result, Tracing}`
     (control frames prioritized over tracing/progress)
2. **Master ↔ Destination**: SSH exec channel `stdin`/`stdout` (same framing and multiplexing, plus
   optional `RcpdToMaster::DirectConnectFailed` and `MasterToRcpd::SshFallbackEndpoints` in
   `--remote-transport=auto`)
3. **Source ↔ Destination**: Two TCP ports on source (destination connects to both, either directly
   or through SSH port forwards):
   - **Control port**: Bidirectional TCP for directory metadata and coordination
   - **Data port**: A pool of TCP connections for file transfers (each connection is reused for
     multiple files, striped round-robin across `source_data_addrs` when multiple SSH tunnels are
     active)

**SSH Data Transport (`--remote-transport=ssh|auto`):**

Direct TCP (`--remote-transport=tcp`) remains the default. When `--remote-transport=ssh` or `auto`
is selected, data never relays through the master host in 3-host topologies:

- **Same-host (`host:/a host:/b`, shared session or verified co-located peers)**: Direct loopback
  (`127.0.0.1`, or `::1` under `AddressFamily inet6` in `ssh` mode), no SSH data tunnels, and no
  slow-path warning. Identical `(user, host, port)` reuses one SSH master session, establishing
  co-location directly. When two independently opened remote sessions resolve via `ssh -G` to the
  same effective `(hostname, port)` and proxy route, master probes both sessions and verifies
  `PeerIdentity::matches` before taking the same-host shortcut (and disables destination agent
  forwarding once confirmed); mismatched or unverified peers stay 3-host so load-balanced aliases
  that land on different backend nodes never take the same-host shortcut.
- **Download (`remote:/src /local/dst`)**: Master probes `source sshd`'s live `SSH_CONNECTION`
  endpoint and binds `source rcpd` to the forwarding server's actual loopback family (falling back
  to client SSH config only if the probe is unavailable), independently of the local client's
  `AddressFamily`. It then opens `1..=K` parallel `-L` local port forwards over OpenSSH mux v4
  (`MUX_C_OPEN_FWD`) on the client's configured loopback family (`127.0.0.1` or `::1`, with fallback
  under `AddressFamily any`) to the source host's `tunnel_target_ip`, reusing the prepared source
  SSH session as tunnel #1 and spawning up to `K - 1` independent `ssh -M -N` sessions (attempting
  node pinning via `ssh_node_endpoint` and verifying `PeerIdentity` before advertising each extra
  port, degrading gracefully down to `1`).
- **Upload (`/local/src remote:/dst`)**: Local `source rcpd` binds the client's loopback family
  (since the local `ssh` client dials it), while master probes `destination sshd`'s live
  `SSH_CONNECTION` endpoint and opens `1..=K` parallel `-R` remote port forwards (`<loopback>:0`,
  receiving `MUX_S_REMOTE_PORT`, cancelled via `MUX_C_CLOSE_FWD` with original listen port `0`) on
  the forwarding server's actual loopback family (or falling back `127.0.0.1` → `::1` when the
  server endpoint could not be probed), reusing the prepared destination SSH session as tunnel #1
  and verifying `PeerIdentity` on extra tunnels `2..=K`.
- **3-Host (`host1:/src host2:/dst`, distinct or unverified remote peers)**: Master resolves
  `host1`'s effective `(user, hostname, port)` locally with `ssh -G` and passes it (together with
  `dest_root`, `source_peer`, and `source_node_endpoint`) in `DestinationTransport::DirectSsh` or
  `DestinationTransport::AutoDirectSsh`, and passes `--ssh-tunnels=<K>` to destination `rcpd` so
  `RemoteResources` reserves and caps local SSH tunnel handles within `RLIMIT_NOFILE`. The
  destination daemon on `host2` opens `1..=K_admitted` parallel `-L` tunnels directly to `host1`
  (trying `source_node_endpoint` first when reachable, requiring positive `PeerIdentity`
  verification on tunnel #1 — failing closed with an actionable routing error on mismatch or probe
  failure, while allowing SSH port translation when Linux `boot_id`, `net_ns`, and `hostname` all
  match — and verifying extra tunnels `2..=K_admitted` before advertising their ports).
- **Automatic fallback (`--remote-transport=auto`)**: Destination first probes direct TCP to
  `source_control_addr` using `--remote-direct-probe-timeout-sec` (capped at
  `--remote-copy-conn-timeout-sec`). If TCP connect succeeds, that socket becomes the control
  connection with zero extra round trips and the transfer commits to direct TCP (it does not fall
  back to SSH later if a firewall permits the control port while blocking `source_data_addrs`; such
  asymmetric firewalls require `--remote-transport=ssh` or `--port-ranges` covering both listeners).
  If the control probe fails before connecting, destination either requests master-managed tunnels
  (`DirectConnectFailed` → `SshFallbackEndpoints`) for download and upload, or opens direct
  `host2 → host1` tunnels for 3-host copies.
- **Dry-run (`--dry-run`)**: Only forwards the control port over tunnel #1 (`K = 1`); no data
  forwards or extra SSH connections are opened. Source sends `SourceMessage::DryRunReady`
  immediately after accepting the control stream and before starting traversal so plaintext
  (`--no-encryption`) destinations distinguish an active preview (`peer_connected = true`) from an
  SSH forward denied before reaching source (`peer_connected = false`).
- **Notice and TLS**: Any non-same-host transfer over SSH emits a default-visible `rcp::notice`
  warning reporting the active tunnel count (and direct-TCP error reason on `auto` fallback).
  Source↔Destination mutual TLS 1.3 remains enabled by default over SSH tunnels unless
  `--no-encryption` is passed.

**Connection Establishment Order:**

1. Source rcpd emits `RCP_READY` on stderr and switches `stdin`/`stdout` to binary framing; master
   immediately starts source stdout demux and stdin heartbeats.
2. Destination rcpd is configured from source `F/E`, emits matching `RCP_READY`, and master starts
   destination stdout demux and stdin heartbeats.
3. Master sends `MasterToRcpd::Hello(MasterHello::Source)` over source SSH stdin with src/dst paths.
4. Source rcpd starts TCP listeners (control + data) and sends
   `RcpdToMaster::SourceHello(SourceMasterHello)` over SSH stdout with both addresses.
5. Master sends `MasterToRcpd::Hello(MasterHello::Destination)` over destination SSH stdin with
   source addresses.
6. Destination rcpd connects to source's control port.
7. Destination opens a pool of connections to source's data port; files are streamed over these
   pooled connections (the `size` field in each header delimits file boundaries).

### 1.4 Security Model

Master ↔ rcpd control and tracing run inside the authenticated, encrypted SSH exec channel (no
master-facing TCP port is opened). Source ↔ Destination TCP connections are encrypted and mutually
authenticated using TLS 1.3 with self-signed certificates and fingerprint pinning. TLS 1.3 is pinned
in the config (TLS 1.2 is never negotiated) — see the [Cipher Suites](security.md#cipher-suites)
section of security.md.

**Security Architecture:**

- SSH authenticates, deploys, and carries all Master ↔ rcpd control and tracing traffic
- Source and destination each generate an ephemeral self-signed certificate (master needs no TLS
  certificate)
- Each rcpd outputs its certificate fingerprint in `RCP_READY` on stderr (read by master via SSH)
- Master distributes fingerprints over SSH stdin to source/destination for mutual TLS authentication
- All Source ↔ Destination TCP connections use TLS 1.3 with mutual certificate fingerprint
  verification

**Security Properties:**

- **Confidentiality**: Master↔rcpd protected by SSH; Source↔Destination encrypted with AES-256-GCM
  or ChaCha20-Poly1305
- **Authentication**: SSH authenticates Master↔rcpd; mutual certificate fingerprint verification
  prevents unauthorized Source↔Destination connections
- **Forward secrecy**: Ephemeral key exchange on both SSH and TLS 1.3
- **Integrity**: AEAD ensures data cannot be tampered with

**Opt-out:**

- Use `--no-encryption` for trusted networks where performance is critical. It disables TLS and
  certificate authentication on Source ↔ Destination TCP connections; Master ↔ rcpd remains
  protected by SSH.
- See [security.md](security.md) for detailed threat model and best practices

### 1.5 Scoped Performance Timings

`--timings=PREFIX` is propagated to both daemons, which aggregate timing spans locally and write
their own `.timings.json` files on their respective hosts at shutdown. These measurements are not
sent over the SSH `stdout` tracing/protocol stream. Startup notices follow the existing
readiness/logging rules. `--timings-detail` enables per-file, metadata, and tracker scopes; ordinary
copies collect neither summaries nor elapsed timelines unless requested. `--chrome-trace`
additionally produces a separate `.scopes.json` elapsed timeline. See
[the profiling reference](../README.md#scoped-timings) for the schema and scope API.

The source scopes have these boundaries in both the hardened and dereferencing walks:

| Scope                                                                       | Measured interval                                                                                          |
| --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `source.discovery`                                                          | Root resolution through `DiscoveryComplete` submission.                                                    |
| `source.directory.scan`                                                     | Directory opening and metadata capture through Begin admission, enumeration, dispatch, and End submission. |
| `source.directory.wait_ready`                                               | Waiting for `DirectoryReady` or rejection.                                                                 |
| `source.directory.wait_release`                                             | Reserve admission waiting for ended siblings to release peer and local lifetime ownership.                 |
| `source.directory.wait_resources`                                           | Waiting for a normal directory group or the sequential reserve.                                            |
| `source.discovery.wait_credit`                                              | Waiting for unacknowledged Begin admission.                                                                |
| `source.files.drain`                                                        | File-task and destination completion after discovery.                                                      |
| `source.file.wait_stream`, `source.file.wait_open`, `source.file.wait_iops` | Detailed waits for stream, open-file, and IOPS admission.                                                  |
| `source.file.open`, `source.file.send`                                      | Detailed data-file opening and sending.                                                                    |

The destination's detailed scopes use the `destination.file` prefix: `wait_open`, `wait_rate`,
`parent`, `plan`, `wait_iops`, `create`, `receive`, `metadata`, `drain`, and `complete`. These cover
file and rate admission, parent resolution, destination classification, IOPS admission, creation,
payload reads and writes, metadata, discarded payloads, and tracker completion respectively.
Completion includes waiting for the tracker and any resulting directory finalization. Under a
directory it created, the receiver reserves the file's IOPS budget and attempts exclusive creation
directly. When a complete nonempty payload is already buffered and fits one write,
`destination.file.create_and_write_buffered` covers the combined operation. Other files use separate
`create` and `receive` scopes. Root files and files in reused directories run `plan` first. An
occupied name can add a `plan` sample and one create retry.

The destination's coarse per-directory manifest scopes separate build admission, inventory, and
publication. `destination.manifest.wait_build` measures acquiring the manifest-build semaphore.
`destination.manifest.inventory` covers the cap check, capped enumeration, and child metadata
lookups for a reused directory, including empty fallback results. `destination.manifest.flush`
covers chunking, send-lock acquisition, all manifest chunks, and the Ready flush, including
announcements with an empty manifest. It ends after releasing the send lock, before tracker
bookkeeping or resulting directory finalization. The build permit spans announcement and any
resulting inline finalization.

`destination.directory.prepare` covers trusted root-parent resolution and directory creation or
reuse, including lockdown and metadata admission. Nonroot parent dependencies and A admission occur
outside this scope. It ends before tracker registration and Ready publication; directories rejected
because an ancestor failed have no preparation sample. Independent preparations overlap, so their
cumulative duration is not serial control-reader wall time. `destination.directory.wait_parent`
measures secured-parent dependency waits (also for symlinks), `destination.directory.wait_prepare`
measures A preparation admission, and `destination.directory.wait_job` measures control-reader
backpressure at the P owned-job limit.

`destination.directory.finalize.control`, `.announce`, and `.data` measure claimed finalization
cascades triggered by control messages, Ready publication, and data-worker completion respectively.
Each sample starts after the initial claim and includes filesystem finalization and ancestor
bookkeeping. The control receiver sends release and Done messages outside these scopes. Calls
without an eligible claim produce no sample; counts represent cascades, not directories. Scope
creation and recording run outside the tracker mutex. Control-triggered finalization occupies the
serial control reader. Preparation and announcement-triggered finalization run in owned directory
jobs; rejection-triggered finalization uses the same `.announce` origin. Directory-job and
data-triggered finalization can overlap other work.

`destination.tracker.access` measures synchronous access to the shared directory-tracker state,
including mutex acquisition and short bookkeeping. Its sample is recorded after unlocking, so timing
aggregation never holds the tracker mutex. `destination.directory.finalize.metadata` measures
applying final directory metadata; `destination.directory.finalize.prune` measures each attempt to
remove an empty traversal-only directory. A failed removal, including a nonempty directory, finishes
its prune scope and proceeds to metadata application. Prune counts are attempts, not counts of
removed directories. Each finalization attempt has its own sample, including ancestors completed by
a child. Finalization runs outside the tracker mutex in the caller that claimed it.

Shared `source.metadata.<operation>.<phase>` and `destination.metadata.<operation>.<phase>` scopes
separate rate admission, concurrency admission, and execution; the blocking helper also records
worker queueing. Their [phase definitions](../README.md#scoped-timings) also describe cancellation
boundaries. Filesystem execution includes closure work and can overlap the higher-level file or
directory scopes.

`operation` covers each process's main operation; `local.copy` covers each local copy invocation.
Interrupted scopes report their interruption. Durations include async suspension and can overlap;
they are cumulative rather than additive wall time. Compare scan and wait scopes with the elapsed
timeline when assessing concurrency.

When branch admission is full, `source.directory.scan` also includes sequential descendant work.

## 2. Protocol Messages

Revision-keyed [wire fixtures](../remote/tests/fixtures/README.md) check representative serialized
payloads in both directions. They complement the protocol-state and remote integration tests; they
do not establish semantic or daemon-spawn compatibility.

### 2.1 Handshake Messages

**`MasterToRcpd`** (Master → rcpd, SSH `stdin` stream)

- **Purpose**: Envelope carried over each daemon's SSH `stdin` pipe.
- **Variants**:
  - `Hello(MasterHello)`: Configuration and connection details sent once after daemon readiness.
  - `Heartbeat`: Periodic keepalive frame sent while `--remote-keepalive-sec > 0` (every
    `keepalive_sec / 3`), starting immediately upon daemon spawn so daemons detect a vanished master
    within `--remote-keepalive-sec` without relying on `sshd`'s `ClientAlive` settings (§6.3, §8.4).
  - `SshFallbackEndpoints { control_addr, data_addrs }`: Sent once to destination `rcpd` after
    `RcpdToMaster::DirectConnectFailed` in download/upload `--remote-transport=auto`, supplying the
    master-opened loopback tunnel endpoints.

**`MasterHello`** (inside `MasterToRcpd::Hello`)

- **Purpose**: Provide configuration and connection details. After sending `Hello`, the master sends
  only periodic `Heartbeat` frames (and at most one `SshFallbackEndpoints` frame in download/upload
  `auto` fallback) until tearing down (closing `stdin` for EOF); each rcpd keeps a reader on `stdin`
  as its unified liveness watchdog for the whole operation (§6.3).
- **Variants**:
  - `Source { src, dst, dest_cert_fingerprint, filter, dry_run, capture, accept_timeout_sec }`:
    Tells source rcpd what to copy
    - `filter`: Optional filter settings for include/exclude patterns (source-side filtering reduces
      network traffic)
    - `dry_run`: Optional dry-run mode (brief, all, or explain) for previewing operations without
      transferring files
    - `capture: ExtendedMetadataCapture { file_acl, dir_acl, root_acl_notice }`: what EXTENDED
      metadata the source must read beyond the `stat` it already does — currently POSIX ACLs. The
      first two are per-entry reads whose bytes are sent; `root_acl_notice` buys one read on the
      ROOT whose only product is a log line. See §2.5.
    - `accept_timeout_sec`: Seconds the source waits for destination's control connection, widened
      beyond `--remote-copy-conn-timeout-sec` when SSH tunnel bring-up or `auto` direct-TCP probing
      precedes destination connect.
  - `Destination { source_control_addr, source_data_addrs, server_name, preserve, source_cert_fingerprint, dry_run, transport }`:
    Tells destination where and how to connect (control address, pooled data address list striped
    round-robin, and `DestinationTransport` mode: `Direct`, `MasterSshTunnel`, `AutoMasterSsh`,
    `DirectSsh`, or `AutoDirectSsh`). Note: empty directory cleanup decisions are communicated
    per-directory via `keep_if_empty` in `DirectoryBegin` messages rather than a global flag.
    - `dry_run`: The master's explicit preview-only policy. Destination starts no data workers and
      waits for `DiscoveryComplete(false)` over control, then sends `DestinationDone`. Source must
      receive that acknowledgement; premature control EOF is a failure. The data connection timeout
      does not limit dry-run traversal.

**`RcpdToMaster`** (rcpd → Master, SSH `stdout` stream)

- **Purpose**: Multiplexed envelope carried over each daemon's SSH `stdout` pipe. Control frames
  (`SourceHello`, `DirectConnectFailed`, `Result`) are prioritized ahead of `Tracing` frames.
- **Variants**:
  - `SourceHello(SourceMasterHello)`: Source's TCP server details (`control_addr`, `data_addr`,
    `tunnel_target_ip`, `server_name`, `peer_identity`, `ssh_node_endpoint`) for destination
    connection, SSH forwarding, and load-balanced peer verification.
  - `DirectConnectFailed { reason }`: Sent by destination `rcpd` when its `AutoMasterSsh` direct-TCP
    probe fails and it needs master-managed SSH tunnels.
  - `Result(RcpdResult)`: Final success/failure status and statistics.
  - `Tracing(TracingMessage)`: Remote tracing logs, `rcp::notice` messages, and progress snapshots.

**`RcpdResult`** (inside `RcpdToMaster::Result`)

- **Purpose**: Report final success/failure status and statistics
- **Variants**:
  - `Success { message, summary, runtime_stats }`
  - `Failure { error, summary, runtime_stats, peer_connected }`: `peer_connected` is true once the
    source↔destination control session (including TLS authentication when enabled) was established;
    when true, the master awaits the peer's terminal result without applying the pre-connection
    peer-failure grace cutoff so partial copied/skipped counts survive.
- **`runtime_stats`**: what the rcpd process used, measured by itself just before it reports: user
  and kernel CPU time in milliseconds, peak resident set size in bytes, voluntary and involuntary
  context switches, and minor and major page faults. Every figure covers the whole process. The
  master prints them per role with `--summary`. The set of fields is not negotiated: master and rcpd
  are always the same build.

### 2.2 Source → Destination Messages (Control Stream)

**`DirectoryBegin { src, dst, metadata, is_root, keep_if_empty, admission }`** opens one directory
discovery record. `admission` is `Normal` or `Reserve`, charged against the corresponding negotiated
directory lifetime capacity (§7.8). The source captures metadata and ACLs from the held directory
before sending Begin, and flushes the parent's Begin before work that can send descendants. The
destination validates lifetime admission and reserves the identity and root claim before filesystem
work, then runs an owned directory job. Admission does not wait for descriptor capacity in the
control reader: an over-capacity or invalid reserve Begin is a protocol error. At most P jobs remain
owned, including finished jobs awaiting reaping, parent waits, announcements, and immediately
eligible finalization. Each job waits for its parent's secured creation or rejection before
acquiring one of the destination's A preparation slots (§7.8). Secured publication installs the held
directory descriptor and reused-directory lockdown before waking children, independently of Ready.
Preparation releases its slot before manifest building, replies, or finalization. Pending jobs
retain early End and child outcomes and may commit after DiscoveryComplete. No job waits for a
future End or child outcome to release P.

`keep_if_empty` retains roots, direct filter matches, reused directories, and ordinary unfiltered
empty directories; a new directory traversed only to find filter matches can be pruned if empty
after completion.

**`DirectoryEnd { src, dst, entry_count }`** seals that directory's discovery. `entry_count` is the
checked number of admitted direct-child obligations, including children that later fail. It excludes
entries filtered or rejected before admission. End follows submission of all its direct children; an
admitted child directory's Begin can arrive after its parent's End. End does not imply that file
data or child directory finalization has finished.

**`DiscoveryComplete { has_root_item }`** closes structural discovery after all directory workers
have joined, every Begin has an End, and no worker can submit more directory or symlink messages.
Files can still be in flight. A dry run or filtered root sends `has_root_item=false`. A root file
can send its data header after `DiscoveryComplete(true)`.

**`Symlink { src, dst, target, metadata, is_root }`** reports a discovered symlink. A nonroot
outcome completes one admitted parent entry. A root outcome completes root processing. Target-read
failures occur before admission: nested failures are collected without increasing the parent count,
while a root failure aborts. The receiver handles one symlink inline, waiting for its already
admitted parent's secured creation or rejection; independently driven directory jobs keep this
bounded wait progress-safe without retaining a symlink queue.

**`FileSkipped { src, dst }`** completes one admitted child whose type can no longer be asserted or
whose file cannot be opened before a data header starts. This includes an admitted nested directory
that cannot be opened or described. A root-file pre-header failure aborts. Once a file header may
have started, a send failure is fatal and cannot be compensated by a skip.

**`FileUnchanged { src, dst }`** completes one admitted file whose destination manifest entry calls
for a successful skip; the destination records `files_unchanged`. No data is sent.

**`DryRunReady`** is sent immediately by source on a dry-run control connection before traversal
begins so plaintext (`--no-encryption`) destinations observe an end-to-end protocol frame and mark
`peer_connected = true` before the preview walk starts. Receiving `DryRunReady` on a non-dry-run
transfer is a protocol error.

Control messages are serialized and flushed at progress boundaries (§7.4). Sibling directory
discovery, file streams, and control replies may interleave. The source admits each child and
increments its parent's final count in one step, so each admitted entry has exactly one terminal
obligation or causes a fatal session abort.

### 2.3 Destination → Source Messages (Control Stream)

**`DirectoryLimits { normal, reserve }`** is the initial framed object on a normal copy's
destination-to-source control stream, before any `DestinationMessage`. Both capacities are nonzero;
the source also requires normal capacity of at least two, leaving room for a scan and a pending file
parent. It intersects these capacities with its own endpoint limits before admitting directories.
Dry runs omit this header and perform no directory lifetime admission.

**`DirectoryManifestChunk { dst, entries }`** carries destination entries from a reused directory
for unchanged-file comparison. Each chunk fits the 8 MiB framed-control limit. Chunks and the
matching `DirectoryReady` are contiguous under one send-stream lock hold, so Ready means the source
has the complete manifest. Different directories may announce in any order. Manifest building runs
outside the control receive loop, with one build slot and the configured
`--overwrite-manifest-max-entries` cap (default 5,000,000). Within that builder, at most the
destination leaf capacity A child lookups run concurrently. Each lookup acquires PendingMeta
admission and classifies through a held child descriptor; only owned metadata enters the manifest.
Started blocking lookups retain their admission until any abandoned descriptor output closes. Entry
order can vary, and failed child lookups are omitted so the source transfers those entries. A zero
cap, failed enumeration, or over-cap directory starts no child lookups. The build permit remains
held through manifest/Ready publication and resulting inline finalization. Fresh directories,
inactive comparison modes, and directories above the cap send no chunks. A failed announce task or
caught unwind panic publishes a fatal error and cancels the receiver promptly; it cannot leave a
source waiting indefinitely for Ready. Parent and capacity waits observe teardown. On source control
EOF, outstanding directory jobs are aborted and joined if Done has not completed; EOF never drains
pending creation into a successful transfer.

**`DirectoryReady { src, dst }`** says the directory was created or reused and can receive file
data. Its manifest chunks have already been flushed. File jobs wait for their own directory's Ready
before comparison or data open. Directory and symlink discovery can proceed without parent Ready.
The destination marks Ready flushed before considering that directory complete, even if End and all
children have already arrived. Ready returns the source's Q-bounded unacknowledged-Begin credit, not
its directory lifetime credit.

**`DirectorySkipped { src, dst }`** rejects a Begin whose creation failed, whose ancestor was
rejected, or whose existing non-directory is retained by `--ignore-existing`. In a live session the
destination sends exactly one Ready or Skipped per Begin. On fatal teardown the control connection
can close instead; source admission and readiness waiters are closed so no task waits for an
acknowledgment that will never arrive. A rejected Begin settles one entry in its successful parent.
The destination records exact rejected Begins and failed ancestor prefixes. Each already submitted
descendant Begin receives its own Skipped, and each rejected Begin still needs one End. Neither
descendant outcomes nor End settle the ancestor a second time.

**`DirectoryReleased { src, dst }`** returns the destination's share of a directory lifetime credit.
For an accepted directory it follows logical finalization and closure of the last held directory
descriptor or alias, including blocking operations and rollback guards. For a rejected directory it
follows rejection and closure of any preparation or rollback descriptors; its End may still be
outstanding. Skipped, like Ready, returns only the separate unacknowledged-Begin credit. The source
reuses lifetime capacity only after both this release and its own last descriptor owner have
released their shared credit. Unknown, mismatched, or duplicate releases are protocol errors. Fatal
teardown may close control without sending outstanding releases. Local descriptor owners still
retain their leases through closure; source admission gates close instead of waiting for those
acknowledgements.

**`DestinationDone`** is sent once after valid discovery completion, terminal root processing, and
completion of every accepted directory, with every admitted Begin's Ready or Skipped flushed. The
control receiver is its sole sender and first flushes every DirectoryReleased. Directory jobs and
data workers notify logical completion but never send Done. The acknowledgement gate survives
compaction of rejected-directory discovery history. Done triggers source shutdown; a fatal error
cannot be replaced by Done.

**Fatal connection and framing rules.** Before completion, clean data-header EOF and transport peer
closure are fatal truncation, including TLS closure without `close_notify`; either is benign only
after logical completion or once teardown is underway. Final success additionally requires Done to
have been sent and its stream closed successfully. Invalid or oversized header frames, decode
errors, TLS protocol faults, and short file bodies are fatal. The destination reports incomplete
transfer as failure even if no specific operation error was recorded, preserving a known connection
failure when available. A fatal data-worker error promptly closes the destination control send side
so a source waiting on Ready or admission wakes. TLS handshakes and data connection attempts retain
configured deadlines and teardown cancellation. Errors are published before awaited cleanup; a
failed stream close cannot mask the original send error.

### 2.4 File Transfer Messages (Data Connections)

**`File`** (Source → Destination, on data connections)

- **Purpose**: File header followed by raw file data
- **Fields**: `src`, `dst`, `size`, `metadata`, `is_root`
- **Format**: Length-delimited serialized header, then raw bytes (exactly `size` bytes)
- **Connection model**: Connections are pooled and reused for multiple files. The `size` field
  delimits file boundaries within a connection. Destination reads headers in a loop until EOF.

Remote file data is always streamed as bytes between `rcpd` processes. Consequently,
`--reflink=auto` and `--reflink=never` have the same remote behavior, and the option adds no field
or other change to the wire protocol.

### 2.5 Entry Metadata and POSIX ACLs

Every message that describes an entry (`DirectoryBegin`, `Symlink`, `File`, and each `ExistingEntry`
in a `DirectoryManifestChunk`) carries a `Metadata`:

```rust
struct Metadata {
    mode: u32, uid: u32, gid: u32,
    atime: i64, mtime: i64, atime_nsec: i64, mtime_nsec: i64,
    acls: WireAcls,
}

enum WireAcls {
    /// No ACL information at all: not captured, or unreadable at the source.
    Unknown,
    /// Read from the source entry: authoritative, including the "has none" case.
    Captured {
        access:  Option<Vec<u8>>,   // system.posix_acl_access
        default: Option<Vec<u8>>,   // system.posix_acl_default (directories only)
    },
}
```

**The ACL fields carry the SOURCE kernel's bytes verbatim.** They are never parsed, rebuilt or
reordered in flight. POSIX.1e requires canonical entry order (`USER_OBJ`, named users by ascending
uid, `GROUP_OBJ`, named groups by ascending gid, `MASK`, `OTHER`) and the kernel rejects anything
else with `EINVAL`, so passing through what the source kernel already validated sidesteps the
problem entirely. The on-disk format is defined little-endian (`__le16`/`__le32`), so it is portable
across hosts as-is; the destination kernel validates on `fsetxattr`. rcp requires an exact rcp/rcpd
version match (see [remote_copy.md](remote_copy.md)), so adding these fields has no back-compat
cost.

**`Captured` with `None` means "the source has no such ACL", which the destination reproduces by
REMOVING the attribute — not by leaving the destination alone.** A destination directory's default
ACL is inherited by every entry created beneath it, including ones rcp creates itself, so "do
nothing when the source has no ACL" would hand the copy permissions the source never granted.

**`Unknown` means the wire carries no ACL information, and the destination must NOT touch the
destination entry's ACLs.** It arrives in exactly two situations: the master never asked for ACLs
(`capture.file_acl` / `capture.dir_acl` both false — the destination's `preserve` then never applies
them either; `capture.root_acl_notice` is irrelevant here, since it buys a log line and never
populates a `Metadata`), or the source COULD NOT read them (a committed directory that failed to
open — that entry's copy is already recorded as an error on the source). The distinction from
`Captured` all-`None` is load-bearing: collapsing the two turns a source-side read failure into an
authoritative clear, permanently stripping a REUSED destination directory's access and default ACLs
on a copy that is already failing. The destination implements `Unknown` by disabling ACL
preservation for that one entry's metadata application; for a strict-mode locked reused directory
that restores the directory's ORIGINAL default ACL (the lockdown snapshot) — the same outcome as
`d:acl` off.

**Which messages actually carry ACLs:**

| message                                                              | carries                    | why                                                                                                                                 |
| -------------------------------------------------------------------- | -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `File`                                                               | access only, when captured | read from the SAME fd whose bytes are sent, so permissions and contents cannot desync                                               |
| `DirectoryBegin`                                                     | both, when captured        | read from the held directory: `O_NOFOLLOW` in hardened mode, the following cursor in `-L` mode; the default ACL governs inheritance |
| a committed-but-unreadable directory whose ENUMERATION failed (§7.1) | both, when captured        | the directory itself was reachable — only reading its entries failed — so it answers the probe like any other                       |
| a committed directory that could not be OPENED at all (§7.1)         | `Unknown`                  | no fd to read them from and no honest answer to give: the destination leaves the destination directory's ACLs untouched             |
| `Symlink`                                                            | nothing                    | the kernel has no symlink ACL; the settings parser rejects `l:acl`                                                                  |
| `ExistingEntry` (manifest)                                           | `Unknown`                  | the manifest answers `--overwrite-compare`, which has no `acl` term — see the hole below                                            |

Reading a source entry's ACL from the same fd as its payload is the same read-side fidelity rule the
rest of the source walk follows (Guarantee 2 in [tocttou.md](tocttou.md)): a probe by path could be
answered by a different inode than the one whose bytes and metadata are on the wire, pairing one
entry's permissions with another's contents. The `-L`/`--dereference` adapter obtains directory
metadata by path, while ACL capture uses its held enumeration cursor without another open. The
cursor retains directory admission through started ACL syscalls, including cancellation. Its earlier
metadata snapshot is not bound to that cursor.

**A failed ACL read FAILS the entry; it never degrades to "no ACL".** Because `None` means CLEAR,
sending it for an ACL the source could not read would make an `EMFILE`, `EACCES` or `ENOENT` STRIP
the destination's ACLs — including a directory's default ACL, which then governs everything created
beneath it. That is strictly worse than failing, and it mirrors the destination's rule for the same
situation in the other direction (a destination that cannot HOLD an ACL fails the entry rather than
dropping it quietly).

A failed ACL read before admission records an error without adding a child obligation. After
admission, a failed nested file or directory sends one `FileSkipped` if no header has started; a
root without a trustworthy message aborts. An ACL read from an opened file data fd that fails before
its header follows the same skip route. No failure is converted into an authoritative ACL clear.

**`MasterHello::Source { capture: ExtendedMetadataCapture { file_acl, dir_acl, root_acl_notice } }`.**
Only `MasterHello::Destination` carries `preserve`, so without this field the source could not know
whether ACLs are wanted and would have to probe unconditionally — a syscall per entry that `stat`
cannot fold in, on every remote copy including ones that do not want ACLs. With every field false
the source issues no xattr syscall at all — except under `--require-toctou-safe`, which arms the
root notice from the source's own mirrored flag rather than from here, so a strict run still pays
the one root `listxattr` on an all-false capture. Nothing per-entry is reachable that way: only
`file_acl` and `dir_acl` open that door.

`root_acl_notice` arms the one-per-run source-root probe behind the notice in
[acls.md](acls.md#the-source-root-warning), and is independent of the two per-entry flags in both
directions: it is set when the master's `preserve` asks for metadata fidelity **at all**, whereas
they are set when it asks for `acl` specifically — which is exactly the case where there is nothing
to warn about. A remote copy left at the shipped default clears all three. `--require-toctou-safe`
arms the notice too but does not travel here: it reaches the source `rcpd` as its own mirrored flag
and is read from the process-global strict state on the host that runs the probe.

`capture` is deliberately NOT the whole `preserve` struct. The source decides only what to **read**;
the destination remains the sole authority on what is **applied**. Handing the source a `preserve`
would invite a later reader to act on, say, `preserve.file.mode_mask` source-side, which would be a
bug. The master derives `capture` from the same `preserve` it sends the destination, at one call
site, so the two cannot disagree — which matters, because the destination reads an all-`None`
`Metadata` as "clear", so a capture that said `false` under a `preserve` that said `true` would
strip every ACL instead of copying it. This is a wire-format change, not a spawn-argument one.

**Application (destination).** File ACLs are applied through the created file's own fd in
`process_single_file`, directory ACLs through the directory's own held fd when it completes
(`DirectoryFinalization::execute`). Both go through the shared appliers
(`common::safedir::set_file_metadata_owned` / `set_reused_dir_metadata_fd`), so the remote path
inherits the local one's ordering rule: an access ACL is the step that WIDENS the destination from
its owner-only create mode, so it runs last and the `fchmod` before it is narrowed to carry only the
special bits (see §5 and [tocttou.md](tocttou.md)). File finalization and its stream-error handling
are described in §7.6. Neither file nor directory ACL application is a new protocol message: the
wire change is confined to the two `Metadata` fields and the `capture` field.

**Not on the wire: the destination's own containment.** Under `--require-toctou-safe` the
destination `rcpd` also strips the ACLs of every directory it creates and snapshots/restores the
default ACL of every directory it reuses, so nothing created beneath one inherits. That is
destination-local behavior driven by the mirrored flag, not by any message, and it applies whether
or not `capture` asked the source for anything. See [acls.md](acls.md) and [tocttou.md](tocttou.md).

**Known hole (unchanged by ACL transport):** a file the manifest shows as identical under
`--overwrite-compare` (default `size,mtime`) is not transferred and keeps its old destination ACL.
This is the same shape as `mode`, which the default comparison also ignores.

See the [ACL user guide](acls.md) for permission hazards, options, and measured costs, and the
[ACL component design](../common/DESIGN.md#posix-acls) for application ordering and containment.
This section is the reference for wire fields and failure accounting.

## 3. Error Communication

The source sends a terminal message for each admitted child it can finish or skip safely.
`FileSkipped` is the generic pre-header outcome for a child whose type cannot be asserted, including
a failed nested-directory open. `FileUnchanged` records a successful manifest skip. A failure after
a file header may have started corrupts that stream and aborts the session; there is no compensating
skip. The source publishes the original error before stream cleanup. A lost child obligation, worker
panic, canceled worker, invalid protocol state, or control-send failure is fatal. In `panic=abort`
builds, a panic terminates the process; asynchronous cleanup applies to returned errors,
cancellation, and caught unwind panics.

The destination records file, symlink, and directory metadata errors locally and continues under
collect-errors policy. Under `--fail-early` it aborts and promptly signals the source through
control closure. Ready or Skipped is the destination's response to each Begin during a live session;
a hard abort may close the stream before that response. Source readiness waiters then wake with the
published failure. The destination's result remains authoritative for its own filesystem failures;
shutdown symptoms do not replace a recorded cause.

A root that cannot be described or opened sufficiently to send a trustworthy Begin or root file
header aborts. A classified root directory whose data-directory open fails can use the committed
unreadable-directory Begin/End(0) route in collect-errors mode (§7.1). Root classification is
performed once and its snapshot drives filtering, dispatch, and `has_root_item`.
`DiscoveryComplete(false)` represents a dry run or filtered root. Destination rejection of a root
directory or failure to create a root symlink is terminal, allowing valid discovery to finish with a
nonzero result.

## 4. Protocol Flow

### 4.1 Directory Copy Flow

![Directory copy flow sequence diagram](assets/protocol_flow_directory_copy.svg)

<details>
<summary>Text transcript</summary>

This trace has a root file `a` and a child-directory file `b`. Destination has sent DirectoryLimits
and data connections are already available. Source admits and opens the held root, then sends
`DirectoryBegin(root, Normal)`. If the destination reuses it, its manifest chunks precede
`DirectoryReady(root)`. Source can scan the root while the response is in flight. It sends
`DirectoryBegin(child)`, receives `DirectoryReady(child)`, then sends `File(a)` and `File(b)` on
separate data streams as enumeration continues.

At each cursor's EOF the source sends `DirectoryEnd(child, count=1)` and
`DirectoryEnd(root, count=2)`. It sends `DiscoveryComplete(true)` after the discovery workers join.
File `b` can finish afterward; the destination then applies child metadata and completes one root
entry. File `a` finishes, making the root's completed count 2/2; destination applies root metadata.
As their final descriptor owners close, the control receiver flushes DirectoryReleased for both
directories before sending `DestinationDone`. End seals a count, not a data stream.

</details>

### 4.2 Directory Completion

![Directory completion conditions](assets/protocol_directory_completion.svg)

A successful directory finalizes only after Ready and any manifest chunks have been flushed, End has
sealed its direct-child count, and exactly that many children are terminal. The destination keeps
its held directory fd and strict-mode lockdown/default-ACL guards through metadata application or
pruning. Finalization then completes exactly one entry in its parent. DirectoryReleased waits for
the final descriptor owner, including any rollback work, to close. A rejected Begin settles its
parent once and still requires its own End, independently of its lifetime release.

### 4.3 Single File Copy

![Single file copy sequence diagram](assets/protocol_flow_single_file.svg)

<details>
<summary>Text transcript</summary>

Destination pre-opens pooled data connections. Source sends `DiscoveryComplete(true)`, then the root
`File` header and bytes on a pooled stream. Destination finishes the file and sends
`DestinationDone`. Source shuts down its data pool and control send side. Destination data handlers
end on EOF and their workers exit after reconnect fails. Data and control EOF have no guaranteed
order.

</details>

### 4.4 Single Symlink Copy

![Single symlink copy sequence diagram](assets/protocol_flow_single_symlink.svg)

<details>
<summary>Text transcript</summary>

Source sends `Symlink(s, is_root=true)` and `DiscoveryComplete(true)` on the control stream.
Destination creates the link, marks the root terminal, then sends `DestinationDone`. The idle data
handlers end during source shutdown.

</details>

### 4.5 Failed Directory Handling

![Failed directory handling sequence diagram](assets/protocol_flow_failed_directory.svg)

<details>
<summary>Text transcript</summary>

The parent has admitted `bad` and a sibling `good`. Source sends `DirectoryBegin(bad)` and an
already submitted `DirectoryBegin(bad/child)`. Destination rejects both with their own
`DirectorySkipped` replies. Rejection of `bad` completes its parent entry once; the descendant
rejection does not complete that ancestor again. Source stops new work under `bad`, drains started
classifiers, and sends `DirectoryEnd(bad/child, count=0)` followed by `DirectoryEnd(bad, count=1)`.
The sibling continues copying. After every Begin has its End, source sends
`DiscoveryComplete(true)`. Destination waits for remaining admitted work and flushes all lifetime
releases, then sends `DestinationDone`. A fatal failure cancels the session and admission waiters
rather than waiting for missing acknowledgments.

</details>

## 5. Directory Completion and Validation

The destination keeps one typed record per accepted directory. A pending record advances through
Preparing, Secured, and Announced; Secured and Announced own the held fd, metadata, creation
outcome, and rollback guard together. Discovering or Sealed child counts remain independent of that
phase. Finalization moves the complete resource bundle into its job and leaves a Finalizing identity
in the tracker. Each pending parent retains the names of its finalized direct-child directories to
reject duplicate Begins; those names are released when that parent finalizes. Exact rejected Begins
remain until discovery validation, and minimal failed-subtree prefixes classify late outcomes. Valid
`DiscoveryComplete` releases all structural history, retaining only failed prefixes needed by
outstanding data work. Finished tracker records retain no fds or metadata; other descriptor owners
keep their lifetime admission until they close.

| Event                          | Effect                                                                                                                        |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `DirectoryBegin`               | Admit the lifetime and reserve identity before asynchronous creation; children wait for secured parent creation or rejection. |
| `DirectoryReady` flushed       | Open the completion gate after any manifest chunks are on the wire.                                                           |
| `DirectoryReleased` flushed    | Return peer lifetime capacity after finalization or rejection and the last descriptor owner closes.                           |
| `DirectoryEnd(count)`          | Seal discovery; reject a missing or duplicate Begin/End, overflow, or count below already completed children.                 |
| File, skip, unchanged, symlink | Complete one admitted direct child, never exceeding a sealed count.                                                           |
| Child directory finalization   | Complete one parent child after its own metadata/pruning; rejection completes the parent once immediately.                    |
| `DiscoveryComplete`            | Validate that every accepted and exact rejected Begin has End; close structural admission.                                    |

An accepted directory finalizes precisely when Ready is flushed, End has sealed it, and
`completed == expected`. End, last-child, and Ready-flushed handlers all check this condition,
including when they run in either order. A successful claim moves the directory into Finalizing,
reserving its identity while an owned job retains its held own/parent descriptors, metadata, and
complete lockdown guard. Only successful completion of that job advances its parent. Finalizing
paths reject duplicate or late Begin, End, Ready, and child outcomes, including new children beneath
a finalizing parent. DiscoveryComplete can validate the already sealed finalizers while they run.
Cancellation or failure leaves the obligation incomplete and latches teardown; collect-errors
metadata failures still commit after recording their cause. Finalization runs in the claiming
caller.

Tracker bookkeeping uses closure-confined synchronous state access. Filesystem work, control sends,
and ACL recovery happen outside its mutex. The announce path releases the send lock before recording
Ready-flushed. Incomplete discovery on connection loss is fatal; no count or metadata is invented to
turn it into success. Duplicate discovery markers and structural messages after DiscoveryComplete
are protocol errors. The marker can precede a pending Ready or file completion. A false
`has_root_item` is invalid after an observed root; a true value still permits the root file header
to arrive later. Root admission is exclusive across entry kinds and streams: a duplicate root is
rejected before its filesystem work begins. A directory claims its root slot before creation or
reuse.

The destination retains each accepted directory's held fd, source metadata, and strict-mode
lockdown/default-ACL guard until finalization. A new directory stays private until its final
metadata is applied. Under `--require-toctou-safe`, a reused directory is inode-rechecked, taken
over with `fchown` and owner-only `fchmod` (preserving setgid where required), and re-statted before
children are written. Its original owner components and default ACL are captured and restored as
required at finalization. A failed takeover rejects the Begin in collect-errors mode or aborts under
`--fail-early`. Directory metadata errors are collected and parent completion continues unless
fail-early is active. A new traversal-only empty directory is pruned only after all children finish;
roots, explicit matches, reused directories, and ordinary unfiltered empty directories remain.

Destination files are created at `0o600` through their pinned parent and receive final mode only
after every payload write has returned. The root is terminal after its own file, symlink, or
directory outcome. `DestinationDone` is sent once only when discovery is valid, the root is
terminal, no accepted directory is pending or finalizing, and all lifetime releases have flushed.
Structural validation detects count and ordering errors but still relies on version-matched peers to
produce one terminal event per admitted entry; the wire protocol has no per-file IDs or retry
guarantee.

## 6. Connection Lifecycle

### 6.1 Shutdown Sequence

Shutdown is initiated by the protocol message and completed through stream closure:

![Shutdown sequence diagram](assets/protocol_shutdown_sequence.svg)

<details>
<summary>Text transcript</summary>

After valid discovery, root-item, pending-directory completion, and flushed lifetime releases, the
destination control receiver sends `DestinationDone` and closes its control send stream.
`DestinationDone` triggers source shutdown; the source drains its admitted file tasks and preserves
any genuine error they return. It stops and joins the control receiver, then scheduler teardown
cancels the data pool and drops the control send stream. A fatal failure publishes the original
cause, closes admission, and cancels the data pool before joining blocked senders.

EOF ends each current data-stream handler. Its worker exits when shared receiver cancellation or a
failed reconnect stops `DataConnectionPool::connect`. Data-stream EOF and source control EOF have no
guaranteed ordering at the destination.

Data workers and directory jobs publish logical completion to the control receiver. The receiver
services descriptor-release notifications while retaining the same pending framed read. After each
successfully handled source frame, it sends up to 64 queued release frames with one final flush so
ready control traffic cannot starve lifetime credits. A batch never waits for more releases;
lifetime bookkeeping commits only after the flush succeeds. After that flush it returns to the
receive-first selection before considering Done, preserving buffered framing/protocol error
priority. It flushes all releases, sends Done, and exits without requiring source control EOF. There
is no application-level `SourceDone` message or universal bidirectional EOF handshake.

</details>

The Done lifecycle distinguishes Open, Sending, Sent, and Failed. Only the control receiver claims
Sending, after logical completion and all lifetime releases, with no teardown latched. A sticky
completion notification wakes it when a worker finishes the final item. Sent commits while the
writer is held, only after both the Done frame and control-stream close succeed. A send/close error
returns its original cause once; failure or cancellation latches Failed before unlocking the writer.
Cleanup never retries a potentially partial Done frame.

Before returning, the control receiver always joins its owned directory jobs: it drains them after
successful Done, or latches teardown and aborts and joins them on EOF, error, or caught panic. The
daemon task scope owns cancellation of the receiver and its children. Final success requires both
logical completion and Sent, which remain sticky through post-success cleanup. A genuine control
framing or protocol failure takes precedence over previously collected recoverable errors;
teardown-induced closure does not replace a known cause.

One `ReceiverShutdown` handle serves the pool, tracker, and admission/finalization/Done claims.
Dropping an armed claim cancels that handle, waking control, parent, capacity, and connection waits,
including connection-permit acquisition and TLS setup. Cancellation and first-connect-error
insertion share one mutex, so an error observed after cancellation cannot replace the initiating
cause. A failing directory job publishes its error in the same poll before its next await;
unconditional joining retains that cause even when shared cancellation concurrently aborts the job.
Stream cleanup publishes cancellation before waiting for the writer and runs concurrently with the
remaining receiver future. Successful Sent remains sticky through cleanup.

### 6.2 Connection Types and Ownership

**Control Connection (Bidirectional TCP)**

- **Owner**: Destination connects to source's control port
- **Lifetime**: Entire copy operation
- **Usage**:
  - Source → Destination: directory discovery, symlinks, skips, and completion marker
  - Destination → Source: initial limits, manifests, Ready/Skipped responses, lifetime releases, and
    Done

**Data Connections (Pooled TCP)**

- **Model**: Destination opens pool of connections to source's data port; source accepts and sends
  multiple files per connection
- **Lifetime**: Entire copy operation (reused for multiple files)
- **Usage**: Length-prefixed file headers + raw data (size from header determines bytes to read per
  file)
- **Pool size**: Effective streams are `min(--max-files-in-flight, --max-connections)`.
  `--max-connections` remains a separately configurable ceiling with a default of 100.

### 6.3 Process Termination

**Master Orchestrates Shutdown:**

1. Receives `RcpdResult` from both source and destination
2. Closes SSH `stdin` pipes to both rcpd processes
3. Waits for rcpd SSH processes to exit
4. Reports combined results to user

After successful wire results, a failed daemon exit or process wait still fails the invocation. This
includes errors flushing explicitly requested timing artifacts during daemon shutdown. Earlier
operation or protocol failures remain the primary error; teardown still drains tracing receivers.

Optional `RcpdToMaster::Tracing` frames are serialized and checked against the framing codec's
maximum frame length before reaching the shared stdout sink; an oversized or unserializable trace is
discarded with a best-effort `stderr` note rather than closing the control channel, while real
transport errors remain fatal. Once `RcpdResult` has been acknowledged by the stdout writer — or on
a non-timeout pre-result exit such as pre-hello `stdin` EOF where no `RcpdResult` was queued — the
daemon closes message admission on both channels and drains already-accepted trailing traces and
startup notices for up to five seconds before closing the SSH stdout descriptor. Live
tracing/progress producers do not keep this drain open. A frame already being sent completes before
the writer handles shutdown; it is never cancelled and restarted on the same stream. A transport
failure ends forwarding, and grace expiry or caller cancellation aborts the owned writer rather than
detaching it. Forwarding remains best effort: a failed or stalled peer can lose remaining output,
and these failures do not replace the operation result. On a keepalive timeout or failed
`RcpdResult` delivery, the daemon stops the stdout writer immediately instead of running the drain
grace, and the writer skips any queued control frame whose acknowledgement receiver has already been
dropped; if a write had already begun on another worker, aborting the writer cannot retract bytes
already accepted by the OS, and the master still requires a successful daemon exit status alongside
successful wire results. The master keeps its stdout demux tasks alive while waiting for daemon
exit, then gives them their existing final drain grace.

**rcpd Lifecycle Management:**

- **Unified master stdin watchdog**: Each rcpd reads framed `MasterToRcpd` messages from SSH `stdin`
  until the copy operation finishes. Before `MasterHello`, during the operation, and while waiting
  for the final `RcpdResult` to flush, read progress on `stdin` (including advancing chunks of a
  large `MasterHello` and periodic `MasterToRcpd::Heartbeat` frames) refreshes the
  `--remote-keepalive-sec` idle read deadline (when nonzero). During startup and operation, an EOF
  (master exited, killed, or SSH channel closed), an idle timeout (vanished master host even when
  `sshd` has no `ClientAliveInterval`), or a decode/I/O error cancels the in-flight operation and
  reports a failure. While delivering `RcpdResult`, delivery remains tied to actual stdout
  disconnect or the configured liveness policy rather than the five-second trace drain grace (with a
  brief post-EOF ack grace so normal master teardown closing `stdin` right after decoding
  `RcpdResult` cannot race with the writer's flush acknowledgement). A stray post-hello
  `MasterToRcpd::Hello` or unexpected `MasterToRcpd::SshFallbackEndpoints` is logged and ignored,
  while an expected `SshFallbackEndpoints` is routed to the destination connect routine without
  interrupting keepalive monitoring, and an undecodable frame is treated as a channel failure.
- If master dies unexpectedly, rcpd detects it and winds down through the normal return path (so
  strict-mode lockdown guards restore reused-directory ACLs)
- No orphaned processes remain on remote hosts

## 7. Design Rationale

### 7.1 Source Discovery and Entry Accounting

Each source directory owns one cursor, enumerated in batches of at most 64 names. Hardened
classification uses the held parent and one transient handle per observed entry; the handle closes
before its classification credit is released. A hardened directory's own metadata and ACLs come from
its opened directory fd. Symlink target and metadata come from one classification handle. The source
uses each classification snapshot for filtering, dispatch, and manifest comparison. It does not
retain a tree-wide name map or re-enumerate a directory to find files.

The hardened cursor has an independent open file description anchored to the held directory. It
opens `.` relative to that fd, retrying through `/proc/self/fd/<n>` on `EACCES` so readable
directories without search permission can still be enumerated. The retry still requires read
permission and a mounted `/proc`. A type hint alone never authorizes security-sensitive work. Held
ancestors are not closed and reopened by path. The `-L` adapter uses the same single-cursor
scheduling and accounting with its intentional symlink-following behavior. Dry-run performs one
discovery for reporting, sends no Begin, End, or data jobs, and finishes with
`DiscoveryComplete(false)`.

Before its first read, the cursor seeks to the start in the same blocking job that reads the batch,
after any ops-throttle, Begin-credit, control-channel, or blocking-pool waits. This refreshes the
open-time directory-entry cutoff on Btrfs. Later batches keep their position; discovery never
rewinds or performs another traversal. A nonseekable cursor (`ESPIPE`) uses its fresh, unread
description without seeking. Other seek errors follow the cursor-error handling below. The
fd-relative dry-run walker uses the same cursor; the `-L` dry-run walker uses its path-based
iterator.

A cursor that has observed EOF returns an empty batch without consuming an ops token or scheduling
blocking work. Enumeration is terminal: entries added after that EOF are not sought out, and no
extra read probes for later listing errors. A full batch alone does not establish EOF. The blocking
read closes the stream and drops its reference to lifetime credit as soon as it observes EOF, before
returning a short final batch for processing or inline descendant scans. Other owners, including the
directory registry and held parent descriptors, retain lifetime credit as described in §7.8. ACL
reads require an open stream; the production cursor ACL read occurs before enumeration on the `-L`
path. A read failure, or cancellation after the cursor hands its stream to owned work, leaves a
failed cursor, which cannot restart or report successful EOF. Cancelling a batch while it waits for
an ops token leaves the cursor usable. Discovery still sends End after processing all names and
receiving the empty batch.

If a directory is Ready and its manifest proves a file unchanged, discovery sends `FileUnchanged`
without file-task admission, coalescing already-available outcomes as described in §7.4. Other
admitted files enter the bounded file queue. Their jobs wait for Ready and the complete manifest,
then send `FileUnchanged` or acquire a data stream before OpenFile admission, opening the source
data fd, and sending the file. The data fd is checked with `fstat` and supplies the wire header's
size, mode, owner, timestamps, and file ACLs. Both read modes derive the header from the opened data
fd; `-L` still opens by path and follows symlinks. In hardened mode, the by-name open can select a
compatible replacement within the held parent; discovery metadata is a snapshot, not authority for
bytes or permissions sent.

An admitted direct child increments its parent's End count in the same submission step. It
contributes one terminal outcome: successful file or symlink work, an unchanged file, a pre-header
skip, a finalized child directory, or a rejected child Begin. An armed obligation guard aborts the
session if a job exits without a terminal outcome. The executor owns the guard outside the job body
and publishes returned errors or caught panics before dropping it, preserving the original cause.
Source discovery and file completion are separate: End can precede outstanding data work, and
DiscoveryComplete follows joined discovery workers while file jobs may remain. The destination's
completion gate is described in §5.

A committed but unreadable directory can send Begin and End(0) with `keep_if_empty=true` in
collect-errors mode where that behavior applies, including a classified root whose directory open
fails and the `-L` unreadable-directory cases. Its available classification metadata has
`WireAcls::Unknown`. It still consumes exactly one Ready or Skipped response. A root with no
trustworthy Begin aborts. A failed already-admitted nested directory that has no trustworthy
description sends `FileSkipped`. A cursor error after Begin seals the children actually admitted in
collect-errors mode, records the error, and lets siblings and existing file jobs continue;
fail-early aborts.

Concurrent mutation is observed through one cursor, without a filesystem snapshot. Added names may
not be seen. On Btrfs, replacing a file by rename after the initial seek can move its directory
entry beyond the cursor's cutoff, so even a name that existed throughout the copy can be omitted.
The initial seek narrows the window; it does not eliminate it. A FUSE client seek may also leave a
server's separately held backing-directory description unchanged, retaining an earlier cutoff.
Nonseekable streams cannot refresh that cutoff through seeking either. An omitted name is never
admitted and need not produce an error. A name that disappears or changes type after admission fails
or skips safely; a compatible regular replacement may be sent with its own data-fd metadata. The
count describes admitted work, not a stable inventory of the source tree.

### 7.2 Root Item Handling

Root entries have no parent count. Their file, symlink, or directory outcome makes root processing
terminal. `DiscoveryComplete(false)` makes a filtered or dry-run root terminal without destination
filesystem work. A root file header can arrive after `DiscoveryComplete(true)`. A root that cannot
be classified, a failed root symlink target read, or a root-file pre-header failure aborts.

### 7.3 Rejected Directories

A rejected Begin completes one entry in its accepted parent immediately. The destination records
both the exact rejected Begin and failed ancestor prefixes. Already submitted descendant Begins
receive their own Skipped responses, but neither their End nor other subtree outcomes complete the
rejected ancestor again. The source stops new subtree work, drains started classifications before
releasing their credits, emits End for each begun directory, and continues independent siblings. A
fatal abort closes readiness and admission gates so no worker waits for absent replies.

### 7.4 Control Message Flushing

The serialized control sender flushes structural messages promptly. Each directory can retain up to
64 already-available immediate `FileUnchanged` outcomes and their obligation guards. It flushes
before waiting for more classification results, file admission, structural sends, inline descent, or
batch and directory completion; it never waits to fill a group. Frames remain individual, with the
framed writer's existing backpressure bound, and guards complete only after the whole flush
succeeds. Pending-manifest file jobs send individual outcomes. A Q-bounded Begin credit (§7.8) is
acquired before registry insertion and sending; it returns on the matching Ready or Skipped. The
separate directory lifetime credit remains held through DirectoryReleased and local descriptor
closure. The destination keeps manifest chunks and Ready contiguous under one send-stream lock hold.
That publication releases the send lock before recording Ready in the tracker. These boundaries let
the control receiver keep making progress while file jobs wait for Ready or admission.

### 7.5 Data Connection Pooling

Data connections are pooled for efficiency:

- The configured pool ceiling defaults to 100 connections (`--max-connections`), while the effective
  pool size is `min(max-files-in-flight, max-connections)`
- Unlimited legacy file input leaves the configured connection ceiling in force
- Destination opens connections to source's data port up to pool size
- Source accepts connections into a shared pool of available send streams; each file-send task
  borrows the next free connection and returns it for reuse (RAII)
- `size` field in headers delimits file boundaries within a connection
- Avoids connection creation overhead per file

The source sends exactly the header's size from the held data fd. Growth beyond that size is
ignored; premature EOF fails the transfer and discards the stream. The stream becomes reusable only
after exactly that size is sent and message completion succeeds. The size is a snapshot of the
opened file, not a consistent snapshot of its contents.

`--remote-copy-buffer-size` is the maximum transfer buffer and file I/O request size. Its default is
the network profile's buffer size (§8.2): 16 MiB for datacenter and 2 MiB for internet. The
configured value must first fit `usize` on the master and each daemon. The master rejects values
outside its range before connecting; a daemon with a smaller range rejects an oversized
`--buffer-size` during argument parsing rather than clamping it. For accepted values, both source
adapters and the destination use the minimum of the configured value, the file's header size, and
the host's `isize::MAX`, with a one-byte minimum. This representability cap is applied before
narrowing the header's `u64` size. The source uses that limit for its payload buffer. The
destination uses it for its copy scratch, which is also the size of its file writes.

Both source adapters keep the opened standard file whose metadata and ACL describe the header. For a
nonempty file, the async sender takes one initialized buffer after flushing the header: the
connection's kept buffer when the file's chunk fits the retention limit, a new allocation otherwise.
Every read of the file reuses it without resizing or reinitializing it. The sender borrows the
caller's admission guard and passes it explicitly to each blocking read; payload reads need no
ambient admission scope. An intermediate read returns the file and buffer. The final read or EOF
closes the file on that worker and returns only the buffer. Empty files close in a blocking job
without reading. Interrupted reads retry; positive short reads are sent immediately. Reads are
bounded by the remaining advertised bytes. The async writer borrows that buffer until `write_all`
completes, before another read can overwrite it. There is no source Tokio file buffer or copy from
such a buffer into an outer reader, and no read-ahead. A buffer within the retention limit stays
with the connection for the next file; a larger one is released with its file. Empty payloads take
and read nothing. After the header is flushed, payload completion (including empty payloads and
premature EOF) explicitly flushes the transport before the source checks the byte count and returns
the stream to the pool. Read, write, flush, and length errors leave the stream poisoned and keep no
buffer.

For files under a directory the receiver created, one worker can both create the file and write its
contents. The entire nonempty payload must already be in the decoder after its header and fit one
chunk. The worker writes directly from those buffered bytes to the new file descriptor. The next
frame stays with the reader. If exclusive creation finds an occupied name, the payload is available
for the usual skip, overwrite or error handling.

For other files the receiver collects network reads in its copy scratch until a whole chunk, or the
rest of the file, is there, and writes that chunk with one blocking job, which takes the file and
the scratch and returns both. A write is a handoff to another thread, so it is issued per chunk and
not per network read; bytes that arrived with the header come first. The receiver borrows the file's
admission guard and passes it to each blocking write, as the sender does for reads. There is no
Tokio file buffer and no second copy of the payload, and every write has returned before the file's
metadata is applied. An ordinary write error on the final chunk leaves the declared payload fully
consumed and the next frame intact: the file still fails, but default error collection can continue
(§7.6). Read errors, truncated payloads, nonfinal write errors, blocking-worker failures, and
cancelled transfers require discarding the stream. A chunk that never filled is not written; failed
transfers keep no scratch taken for the copy.

Individual file reads and network operations can be shorter; this setting does not promise TCP
packet or TLS record sizes. Larger settings allow more working memory per active transfer: one
buffer of up to the chunk size on each side. The setting is not a process RSS bound or a throughput
guarantee. Neither the argument checks nor the per-file cap check available memory or bound the
combined allocation across active transfers. Excessively large settings can exhaust memory and abort
a daemon; `isize::MAX` prevents unrepresentable byte buffer lengths, not allocation failure.

Both sides lazily keep one initialized buffer per data connection: the source its payload buffer,
the destination its copy scratch. Each grows only to the chunk a file needs. Empty files need no
buffer, and a skipped file's payload that is already in the framed reader is drained without
scratch. The default retention limit is the smaller of `--remote-copy-buffer-size` and the network
profile's buffer size, so a profile's own copy buffer is kept whole, and a copy buffer raised above
the profile's size is not kept without an explicit limit. Retention does not change the configured
I/O maximum.

Use `--remote-copy-buffer-retention-limit=SIZE` to set an independent per-connection limit (for
example, `2MiB` to hold less idle memory under the datacenter profile); `0` disables retention. The
shared daemon configuration forwards `--buffer-retention-limit=BYTES` to both roles, and both use
it: the source for its payload buffers, the destination for its copy scratch. The limit does not
shrink I/O chunks: on either side a chunk exceeding it uses temporary storage for that file, so a
file larger than the limit costs each side one allocation of its chunk. Smaller files and 8 KiB
drains reuse kept storage when their required chunk fits the limit; drains above the limit also use
temporary storage. A limit above the copy chunk does not eagerly allocate that amount.

Reuse matters more than the size of a buffer suggests. Before the copy binaries adopted mimalloc,
the statically linked daemons' allocator used fresh memory mappings for these buffers. A run on a
96-CPU host spent most of its CPU time in mapping, page faults, and unmapping while copying 1 MB
files, with about one file's worth of page faults per file in each daemon. This is historical
pre-mimalloc evidence, not a measurement of the current allocator.

The default keeps a profile's whole copy buffer, so that no file costs an allocation of its chunk on
either side. With the earlier default of 2 MiB for both profiles, a datacenter file between 2 MiB
and 16 MiB cost each daemon one. On one 20-CPU WSL2 host using tmpfs and a loopback with 0.35 ms of
added round-trip delay, remote copies of 8 GiB took 12% less time for 4 MiB files and 7% less for 16
MiB files with whole buffers kept (11% and 5% without encryption). The price is idle memory, counted
below. This is evidence for the default policy, not a throughput guarantee or an optimum for
physical high-bandwidth networks; the override allows tuning for those workloads and for hosts where
idle memory matters more.

Kept buffers have no spare capacity (the destination's is a boxed slice, the source's a vector
allocated at its exact length), so in each daemon they are bounded by the limit times the effective
connection count, `min(max-files-in-flight, max-connections)`. At 32 connections the default ceiling
is 512 MiB per daemon for the datacenter profile and 64 MiB for the internet profile; at the default
100-connection ceiling it is 1600 MiB and 200 MiB. An explicit 2 MiB limit gives the datacenter
profile the internet profile's ceilings. Automatic file concurrency uses the source CPU count (at
least four). Descriptor admission can further limit active file operations but does not reduce the
connection pool: all connections may eventually keep buffers. Retention grows only as connections
encounter payloads that need a buffer, and trades idle memory for fewer allocations and repeated
initializations. In a file transfer a smaller kept buffer is released before a larger one replaces
it. These storage bounds exclude allocator overhead, framing, transport, active temporary buffers,
and transient overlap when a drain grows the scratch; they are not process RSS bounds. Dropping a
transfer future releases its temporary storage, or the buffer it had taken from its connection; kept
storage otherwise lives until the connection drops. Cancelled transfers require discarding the
stream; ordinary final-chunk write errors follow the recovery rule in §7.6.

**Connection lifecycle:**

1. Destination opens N connections to source's data port
2. Each connection handles multiple files (loop reading headers + data)
3. Source sends file header (length-prefixed) + raw data (`size` bytes)
4. After all files sent, source closes connections (destination sees EOF)

**Trade-offs:**

- Efficient reuse avoids TCP handshake per file
- Natural backpressure via pool size limiting
- Slightly more complex error recovery (need to track stream state)

### 7.6 Stream Error Recovery

When processing a file fails, the destination must determine if the connection can continue
receiving more files:

| State            | Cause                                                                             | Recovery                                               |
| ---------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------ |
| **NeedsDrain**   | Error before reading data (e.g., can't create file)                               | Drain `size` bytes, continue with next file            |
| **DataConsumed** | Ordinary final-chunk write error after consuming all payload, or metadata failure | Next frame intact; collect the file error and continue |
| **Corrupted**    | Incomplete payload, read error, nonfinal write error, or blocking-worker failure  | Discard the stream and abort the session               |

**Regular-file finalization.** Each payload write returns before the receiver transfers the same
standard-file owner to the metadata applier. No Tokio file conversion or additional payload flush is
needed. An ordinary final-chunk write failure is `DataConsumed`: the receiver has consumed every
declared payload byte, but the file still fails. Its descriptor and scratch are dropped, no metadata
is applied, and no copied-file or copied-byte counts are added; any partial destination remains in
its private creation mode. Default error collection records the original I/O error, settles the
file's completion obligation, and reads the next frame without draining. A metadata failure is also
`DataConsumed`, but retains the copied counts already added after successful payload writes.
Incomplete payloads, read errors, nonfinal write failures, and blocking-worker failures remain
`Corrupted`. Successful payload completion means the kernel accepted the writes; it does not imply
an `fsync` or durable storage.

Ownership and timestamp preservation may share one blocking worker submission when ACL preservation
is off and timestamp admission is immediately available. Each operation still acquires its own rate
token and concurrency permit and records its own congestion sample. A pending admission returns to
async code with the same file owner and the exact pending future, preserving any paid token and
queue position. Cancellation prevents an unclaimed timestamp operation. Final permission widening
remains a separate operation after the earlier metadata work succeeds.

Recoverable file errors are collected while other files continue unless `--fail-early` is set.
Fail-early latches teardown and returns the original file failure without draining unread payload or
advancing file-completion bookkeeping; a slow or stalled sender cannot delay this failure by
withholding the rejected file's bytes. A failed collect-errors drain corrupts the stream and aborts
the session. Corruption latches teardown before file-completion bookkeeping. Both paths prevent new
pending-directory finalization or Done. A failed copy can therefore leave directories in their
temporary restricted mode; held reused-directory rollback guards still restore their protected
state. Cleanup preserves the original error.

Directory metadata errors are handled analogously in `DirectoryFinalization::execute`: the error is
logged, pushed to the `ErrorCollector`, and processing continues (unless `--fail-early`). The
directory is still marked complete and parent notifications still propagate.

### 7.7 Summary Statistics Authority

The master merges source and destination summaries based on mode:

- **Normal mode**: destination is authoritative for copy/create/unchanged/remove counts (it knows
  what actually landed on disk). Source is authoritative for skip counts (filtered and special-file
  skips happen before items reach the destination).
- **Dry-run mode**: source is authoritative for all counts (destination is idle).

Skip counts (`files_skipped`, `symlinks_skipped`, `directories_skipped`, `specials_skipped`) always
come from the source regardless of mode.

### 7.8 Backpressure and Task Ownership

The source resolves `E = min(F, M)`, where F is the logical file ceiling and M is the connection
ceiling, then `P = E × pending-writes-multiplier` with checked, nonzero arithmetic. For a normal
copy, each daemon resolves one [`RemoteResources`](../common/src/runtime_setup.rs) plan before
readiness, using its effective soft descriptor limit S. Startup attempts to raise the soft limit to
the inherited hard limit without changing the hard limit. A failed raise warns with the
operating-system cause and preserves the original soft limit for admission. File-work concurrency
remains independently bounded. Runtime setup installs the plan's leaf capacity A once in each of the
independent OpenFile and PendingMeta pools; directory negotiation consumes the same plan.
Preview-only startup uses local admission instead (§1.2).

With known S, let T be the semaphore maximum. The calculation uses saturating subtraction and
bounded arithmetic:

```text
B = max(S - E - 32, 0)
A = min(E, F, 4096, floor(B / 12))
G = floor((B - 4A) / 2)
N = min(floor(G / 2), T)
R = min(G - N, T)
```

The four descriptor units per A cover concurrent leaf work across both independent pools and
additional destination preparation work. Each directory lifetime budgets two descriptors. The plan
leaves at least two normal and two reserved lifetimes per leaf slot, with modeled consumption
`4A + 2(N + R) + E + 32 ≤ S`. Normal and reserve traversal share the available directory budget
equally, with any odd remainder assigned to the reserve. Directory capacity is independent of the
pending-file limit: one scanner can retain many ancestor directories. If B is below 12, startup
fails before readiness with a typed `RCP_ERROR` naming S, E, the support reserve, and remedies:
reduce connections/file concurrency or configure a higher hard limit in the affected host's SSH
login/sshd session. Changing only the master's shell limit does not change remote daemon limits. If
raising the soft limit is denied, the startup warning identifies the operating-system cause. The
diagnostic gives the minimum effective limit and exact shortfall.

The source takes the smaller N and R supported by the two endpoints. It then sets its scan capacity
`W = min(A, floor(N / 2))` and pending-work capacity `Q = min(P, N - W)`, using its own A and the
negotiated N. This leaves normal lifetime capacity for both active scans and pending file parents.
For S=1024, E=20, and P=80 at both endpoints, A=20, N=223, R=223, W=20, and Q=80. A lower-limit peer
can reduce W and Q without changing the source-authoritative F/E/P handshake. Reductions of an
explicit file-work policy produce a notice; automatic reductions are visible at info verbosity.

Unknown descriptor headroom is accepted only with a finite user-supplied file limit. It uses
`A = min(E, F, 4096)` and `N = R = min(2(P + E), T)`, with a notice and no descriptor-safety
guarantee. Automatic or unlimited admission with a failed limit query, and any effective zero soft
limit, remain startup errors.

| Work                       | Bound and release point                                                               |
| -------------------------- | ------------------------------------------------------------------------------------- |
| Concurrent scans           | W; normal scans release after an empty batch, reserve scans after their subtree scan. |
| Concurrent classifications | Q and source PendingMeta admission A; both release before descent.                    |
| Normal directory lifetimes | N; credit covers both endpoints until release and last local descriptor closure.      |
| Reserved lifetimes         | R outstanding lifetimes in one subtree; one inline scan pipelines their completion.   |
| Unacknowledged Begins      | Q; credits return on Ready or Skipped.                                                |
| Pending file jobs          | Q; includes waits for Ready, a stream, and completion.                                |
| Destination directory jobs | P; includes parent waits, preparation, replies, and eligible finalization.            |
| Destination preparations   | Destination A; acquired after the parent dependency and released before announcement. |
| Manifest child lookups     | Destination A and PendingMeta admission, within one directory builder.                |
| Cursor batch               | At most 64 names per directory frame.                                                 |
| File streams               | E; a stream is acquired before OpenFile admission A and the data-fd open.             |

![Directory lifetime admission at both endpoints](assets/protocol_directory_resources.svg)

Directory admission reserves credit before opening a held directory and its cursor. Every descriptor
owner retains that credit, including file-job aliases, blocking operations, and receiver rollback
guards. Ready returns only the separate Q-bounded Begin credit. The source retains lifetime credit
until DirectoryReleased arrives and its own final descriptor owner closes; the destination emits
that release only after finalization or rejection and its last descriptor owner closes.

When normal credits are exhausted, admission waits for a normal credit or the single reserve lane.
The lane owns one scan permit while scanning its subtree and never forks. Each directory acquires a
lifetime credit, sends End after its inline descendants have been scanned, and returns without
waiting for file completion or DirectoryReleased. Registry and descriptor aliases retain its credit.
The scan permit returns when the subtree scan ends; outstanding credits retain only lane
exclusivity. If R credits are occupied, admission waits for an ended sibling's credit to return. A
continuation with R reserved ancestors instead reports depth exhaustion immediately: waiting on
those ancestors would prevent the End messages needed to release them. Fatal shutdown closes the
admission gates.

R bounds reserved ancestry below the lane's entry point, not total path depth. Concurrent normal
work affects where that entry occurs. Raising the affected daemon's inherited SSH-session limit
increases available directory capacity. For a nested depth error, collect-errors mode records and
skips that subtree while other work continues; `--fail-early` aborts. Normal inline descent moves
the parent's scan permit into its child and reacquires one after that child returns. After the empty
batch ends enumeration, discovery releases normal scanner resources before joining outstanding
descendant tasks; processing the last nonempty batch can already have scanned inline descendants.

Ready waits stay within Q: each file's parent Begin already owns directory lifetime admission, and
destination preparation/announcement does not need a source file-job permit. The destination
validates Normal/Reserve admission synchronously. It retains each lifetime until both its End and
its flushed release, including a rejected directory released before its End. A Reserve Begin must
have an active, unended reserved parent, or start a new lane when no reserved lifetime remains.
Ended siblings may still own descriptors while the next sibling begins, within R. The receiver
retains the lane root's identity until its last lifetime retires and rejects unrelated reserved
subtrees or normal descendants within the reserved subtree.

The source control receiver never waits for a scan, file permit, or directory worker. Sequential
descent uses a tracked child task that the parent immediately joins, keeping the Rust call stack
bounded. Classified entries are polled independently; descent holds no classification or file permit
needed by a suspended ancestor. Completed handles are reaped continuously. File jobs belong to the
connection, so directory workers can End while payloads remain in flight. Task scopes and
abort-on-drop ownership cover stdin and master-watchdog cancellation. Fatal errors close admission,
cancel readiness waiters and the data pool, and join blocked senders. Started blocking syscalls
retain their descriptor and admission lease until they exit.

The destination admits each file after reading its header, before parent resolution, creation, and
writes. A is endpoint-local; N/R are exchanged on control. Directory and cursor descriptors have a
modeled bound of `2(N + R) + O(1)` per endpoint. This startup model does not account for arbitrary
inherited support fds or later external limit changes and does not impose a fixed total process-fd
or whole-tree memory ceiling.

Recursive removal during `--overwrite` uses the shared removal walker when a destination directory
must be replaced by a file or another entry type. Its directory descriptors are outside N/R lifetime
admission. Low hard limits or extreme destination trees can therefore exhaust descriptors during
recursive removal even when copy discovery stays within its modeled bound.

`--max-connections` defaults to 100 and `--pending-writes-multiplier` to 4. When
`--max-files-in-flight` is omitted, the source chooses `max(available_parallelism(), 4)` and the
destination adopts it. Explicit unlimited leaves the connection ceiling in force.

Source payload reads pass their borrowed OpenFile guard to the repository's explicit-admission
blocking runner. Queued cancellation destroys the file and buffer before releasing the job's
admission; started reads and abandoned returned outputs retain admission until their owned resources
are dropped. The outer OpenFile guard remains held through network writes and final flush.

Destination payload writes use that same explicit-admission boundary. Each chunk transfers the
standard file and scratch buffer to the blocking job; queued cancellation or an abandoned result
drops those resources before releasing admission. A started write retains admission until it
returns. Metadata then consumes the same file owner through individually admitted operations. This
statement covers these source reads, destination writes, and metadata jobs; it does not extend to
every Tokio filesystem operation elsewhere in the program or require a wire change.

### 7.9 Skipping Identical Files

With `--overwrite` or `--ignore-existing`, a reused destination directory can send a fd-relative
manifest of its existing entries. Chunks precede that directory's Ready, remain under the 8 MiB
frame limit, and are omitted when the manifest exceeds `--overwrite-manifest-max-entries` (default
5,000,000). The source compares each admitted file's classification snapshot with the completed
manifest using `--overwrite-compare` (default `size,mtime`) and `--overwrite-filter=newer`. With
`--ignore-existing`, any name collision skips regardless of entry type. A matching file sends
`FileUnchanged`, completing one child entry and incrementing destination-owned `files_unchanged`
without data transfer.

The manifest and source classification are point-in-time observations. A skip leaves the destination
entry untouched even if either side changes afterward. For a file that is sent, the source opens a
data fd under its pinned parent, checks it as regular, and derives the header and ACLs from that
same fd. The destination writes or removes through its pinned parent; a compatible final-component
replacement may be affected, while containment and metadata fidelity remain. A single root-file copy
has no parent-directory manifest, so it transfers the file and lets the destination drain or write
it.

## 8. TCP Configuration

### 8.1 Connection Settings

Both `rcp` and `rcpd` accept CLI arguments for TCP connection behavior:

- `--remote-copy-conn-timeout-sec=N` (`N >= 1`; default: 15; 60 with auto-deployment) - Timeout for
  SSH session setup, remote binary discovery, tilde-expansion HOME lookup, remote version probes,
  deployment command/readiness, each deployment payload-write idle period, cleanup commands, daemon
  SSH exec/readiness, and TCP connection setup; it does not cap total binary-transfer duration, and
  post-EOF deployment verification gets at least 60 seconds
- `--remote-keepalive-sec=N` (default: 120, `0` disables) - Liveness budget for remote connections
  (Source↔Destination TCP keepalive/`TCP_USER_TIMEOUT` and Master↔rcpd SSH `ServerAlive`/heartbeat)
- `--port-ranges=RANGES` (optional) - Restrict TCP to specific port ranges (e.g., "8000-8999")
- `--max-connections=N` (default: 100) - Separately configurable data-connection ceiling; effective
  streams are `min(max-files-in-flight, max-connections)`
- `--max-files-in-flight=N|unlimited` - Explicit master-authoritative file-work policy; a positive
  `N` sets a finite ceiling and `unlimited` removes the user ceiling. When omitted, the source
  resolves `max(std::thread::available_parallelism(), 4)` and the destination adopts it. Finite
  values also clamp effective data connections, while explicit or legacy unlimited input does not
- `--pending-writes-multiplier=N` (default: 4) - Configured pending capacity P is effective streams
  × this multiplier; source admission reduces it to Q when directory headroom requires (§7.8)
- `--network-profile=PROFILE` (default: datacenter) - Buffer sizing profile

Old-version cleanup is idempotent, best-effort cache hygiene. Its deadline bounds the master's local
SSH wait; a remote cleanup command that has already started may finish independently after that
channel is closed.

For explicit policy, the master validates the pending-task product before remote-home lookup or SSH
and configures the source. For automatic policy, the master validates the configured connection
upper bound before remote side effects; source readiness then supplies `F/E`, the source validates
its actual product before readiness, and the master gives the destination the same values and
rejects a mismatch in destination readiness. A directly launched daemon validates before announcing
readiness (`RCP_READY`). File concurrency travels in version-sensitive rcpd spawn arguments and
readiness; directory lifetime limits use the separate initial control header (§2.3). A zero
remote-copy connection timeout is rejected.

### 8.2 Network Profiles

A profile selects one buffer size. It is the TCP socket buffer size wanted on data connections, and
the default for `--remote-copy-buffer-size` and for the retention limit of §7.5; those two keep the
profile's size whatever the sockets end up with.

Whether a socket buffer is requested (`SO_SNDBUF`, `SO_RCVBUF`) is decided by each process for its
own host. A request is not harmless: the kernel cuts it to `net.core.wmem_max` / `rmem_max` without
reporting it and then never changes that buffer, while a buffer nobody asked for is grown by the
kernel up to the last value of `net.ipv4.tcp_wmem` / `tcp_rmem` (for receive buffers only while
`net.ipv4.tcp_moderate_rcvbuf` is on; otherwise they stay at the middle value). A process therefore
requests a buffer only where twice the size the kernel would grant, which is how the kernel accounts
a granted request, exceeds what the kernel would reach by itself. When the limits cannot be found
out or the two are equal, it requests nothing. The growth limits are read from the kernel's
settings. The request limits are found by asking a socket that never connects, because the settings
that state them are not visible inside a network namespace on kernels before 6.12, where they bind a
container all the same.

- The source requests its send buffer on each accepted data connection.
- The destination requests its receive buffer on each data connection BEFORE it connects. A receive
  buffer requested on an established connection does not update the receive-window clamp on older
  kernels without the fix (introduced in Linux 6.11 and backported to some stable kernels). The
  advertised window can remain limited by the initial receive-buffer sizing despite the later,
  larger buffer. With the tested initial buffers it stayed around 64 KiB during transfer. That is an
  observed effective window, not the unscaled SYN window field or a fixed cap across hosts.
- The destination then takes the kernel's "fixed" mark off that buffer again (`SO_BUF_LOCK`, kernel
  5.15 and later), still before connecting. The buffer keeps its size. A buffer that is marked fixed
  when the socket connects makes the kernel choose the connection's window scale for half of that
  buffer, and the scale cannot change later: of the 416 KiB that a stock request limit grants, such
  a connection can never advertise more than 0.25 MiB, where kernels from 6.6 on would otherwise use
  0.30 MiB.
- Control connections, and the directions no file data flows in, request nothing.

The two ends decide independently and nothing about it is exchanged: a source may request while its
destination does not. The source and the destination each log their decision once at info level. The
master makes no data connection and decides nothing. The destination also logs a warning, once,
where its kernel does not grow receive buffers (`net.ipv4.tcp_moderate_rcvbuf=0`) and a data
connection is left with less than the profile's size: no request gets such a host past its limits.
The warning goes out on the `rcp::notice` target, so the master prints it at its default verbosity.

**Datacenter Profile (default):**

- 16 MiB buffers
- Optimized for low-latency, high-bandwidth networks

**Internet Profile:**

- 2 MiB buffers
- More conservative settings for higher-latency networks

### 8.3 Tuning Guidelines

- **Datacenter**: Use default settings for best performance
- **Internet/WAN**: Use `--network-profile=internet` for better behavior on higher-latency links
- **Firewall-restricted**: Use `--port-ranges` to specify allowed ports
- **More remote parallelism**: To exceed the CPU-derived file-work default, increase both
  `--max-files-in-flight` and `--max-connections`; increasing either ceiling alone may leave the
  effective stream count unchanged.

### 8.4 Connection Liveness

Every Source↔Destination TCP connection — control and each pooled data connection — is configured
through one function (`configure_tcp_socket` in the `remote` crate) that applies `TCP_NODELAY`, the
buffer requests of §8.2, and dead-peer detection. Master↔rcpd control and tracing run over SSH and
use the same `--remote-keepalive-sec` budget via two complementary mechanisms:

- **Master → SSH server (`ServerAliveInterval` / `ServerAliveCountMax`)**: The retained `ssh -M -N`
  multiplex master configures `interval = min(ceil(N / 6), i32::MAX)` and
  `count = clamp(ceil(N / interval), 2, 6) - 1` (or `ServerAliveInterval=0` when `N = 0`, overriding
  `~/.ssh/config`). Because OpenSSH increments its unanswered-probe counter before checking
  `> ServerAliveCountMax`, it disconnects after `interval * (count + 1)` silent seconds — matching
  standard budgets such as `120s` (`(20, 5)`) and `5s` (`(1, 4)`) directly while guaranteeing at
  least one probe before disconnecting an idle healthy session (giving a 2-second minimum effective
  OpenSSH bound `(1, 1)` when `N = 1`).
- **Master → rcpd (`MasterToRcpd::Heartbeat`)**: The master sends periodic heartbeat frames over
  each daemon's SSH `stdin` every `N / 3` seconds when `N > 0`, and each daemon enforces an
  `N`-second idle read deadline on `stdin` (refreshed on every non-empty read so large advancing
  frames stay alive) so a daemon whose master host vanishes exits within the budget even when `sshd`
  does not configure `ClientAliveInterval`.

A peer whose HOST vanishes (power loss, severed link, destroyed VM) sends neither `FIN` nor `RST`,
so without detection a read on it never completes: the master awaits `RcpdResult` with no timeout of
its own, and the source↔destination control reads behave the same way, so all three processes hang.

`--remote-keepalive-sec=N` is the budget for noticing this. On TCP connections it arms two options,
and **which ones apply depends on what the connection carries** — the function takes a
`ConnectionKind` (`Control` or `Data`), and it is reached only through one typed entry point for
each kind of socket (a control connection, an accepted data connection, a data socket about to
connect), so no call site can leave that open:

This is not an overall operation deadline. A live host whose userspace `rcpd` is stopped or wedged
can still have its kernel acknowledge TCP keepalives and zero-window probes (and `sshd` can still
answer OpenSSH keepalives), so neither keepalive nor `TCP_USER_TIMEOUT` necessarily fires. The
master can consequently remain waiting for an `RcpdResult`; use external supervision when an
absolute copy deadline is required.

|                                                                                  | source↔destination control | data connections                         |
| -------------------------------------------------------------------------------- | -------------------------- | ---------------------------------------- |
| what they carry                                                                  | source↔destination control | pooled source→destination file transfers |
| `SO_KEEPALIVE` (`TCP_KEEPIDLE` = N/2, `TCP_KEEPINTVL` = N/12, `TCP_KEEPCNT` = 6) | yes                        | yes                                      |
| `TCP_USER_TIMEOUT` = N                                                           | yes                        | **no**                                   |

`SO_KEEPALIVE` probes an **idle** TCP connection — the source↔destination control stream and a data
connection between transfers. `TCP_USER_TIMEOUT` bounds how long **unacknowledged** data may stay
outstanding, which keepalive cannot cover because it never fires while data is in flight.

**Why data connections are excluded.** `TCP_USER_TIMEOUT` cannot distinguish a dead peer from a live
one that has stopped reading: with the receiver's window at zero and every zero-window probe ACKed,
the sender is still aborted when the budget expires. The destination does exactly that — it awaits
its per-file iops reservation *after* reading a file header and *before* reading any of the file's
bytes, so `--iops-throttle 50` on a 10 GiB file at 1 MiB chunks leaves that socket unread for
minutes. Applying the budget there would turn a copy that merely ran slow into a copy that
**fails**.

The consequence is stated rather than glossed: a host that vanishes **mid-transfer** on a data
connection is detected only by the kernel's retransmission limit (`tcp_retries2`, roughly 15
minutes) — the behavior that predates this option, so no regression, but not a 2-minute detection
either. An **idle** data connection is still caught by keepalive after idle + retries × interval.
Note also that on Linux `TCP_USER_TIMEOUT` overrides the keepalive probe count, so `TCP_KEEPCNT` is
inert on control connections and is what actually ends a dead data connection.

Control bookkeeping does not consume filesystem ops tokens. The control reader can still wait for
parent creation, owned directory-job capacity, or inline directory finalization. Filesystem work
behind those waits remains rate- and congestion-limited. If reads stall long enough to fill the
receive buffer and close the window, pending source control writes can reach the user-timeout
budget. Removing the per-message rate charge does not make control connections backpressure-free.

The sub-values are derived from the single budget rather than configured individually, so their
relationship stays correct by construction. `N = 0` disables TCP keepalive, `TCP_USER_TIMEOUT`,
OpenSSH `ServerAlive` probing, and master `stdin` heartbeats/read timeouts, leaving no-delay and
buffer sizing on TCP sockets.

The master mirrors its value into each rcpd's spawn arguments (via `RcpdConfig::to_args()`, like
`--require-toctou-safe` in §1.2); without that the master would recover from a vanished host while
both rcpds kept hanging. Setting a TCP socket option is best effort — one that a platform or
container policy refuses is logged and tolerated, not a copy failure.
