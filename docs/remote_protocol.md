# Remote Copy Protocol Design Document

## 1. Architecture Overview

### 1.1 Three-Component Architecture

The remote copy system consists of three distinct components:

1. **Master (rcp)**: Coordinates the entire operation, runs on the client machine where the user invokes `rcp`
2. **Source (rcpd)**: Runs on the source host, responsible for reading and sending files
3. **Destination (rcpd)**: Runs on the destination host, responsible for receiving and writing files

### 1.2 Component Spawning and Lifecycle

**Spawning Sequence:**

1. User invokes `rcp user@host1:/src user@host2:/dst`
2. Master creates a TCP listener and waits for connections
3. Master spawns source rcpd via SSH: `ssh user@host1 rcpd --master-addr=... --role=source ...`
4. Master spawns destination rcpd via SSH: `ssh user@host2 rcpd --master-addr=... --role=destination ...`
5. Both rcpd processes connect to master via TCP
6. Master identifies them by their declared role (sent via `TracingHello`)

**Special Case - Same Host Copies:**
When source and destination are on the same host, the master:
- Deploys rcpd only once (if needed)
- Starts two separate rcpd processes with different roles
- Both processes share the same SSH session but have distinct connections

### 1.3 Connection Topology

The system uses a **triangle topology** with TCP connections:

```
         Master (rcp)
           /    \
          /      \
     (TCP)      (TCP)
        /          \
       /            \
Source (rcpd)-----(TCP)----Destination (rcpd)
                control + data ports
```

**Connection Details:**

1. **Source → Master**: Bidirectional TCP, used for handshake and result reporting
2. **Destination → Master**: Bidirectional TCP, used for handshake and result reporting
3. **Source ↔ Destination**: Two TCP ports on source:
   - **Control port**: Bidirectional TCP for directory metadata and coordination
   - **Data port**: Multiple TCP connections for file transfers (one connection per file)

**Connection Establishment Order:**

1. Both rcpd processes connect to master (order undefined)
2. Master sends `MasterHello::Source` to source rcpd with src/dst paths
3. Source rcpd starts TCP listeners (control + data), sends `SourceMasterHello` back to master with both addresses
4. Master sends `MasterHello::Destination` to destination rcpd with source addresses
5. Destination rcpd connects to source's control port
6. For each file, destination opens a new connection to source's data port

### 1.4 Security Model

All TCP connections are encrypted and authenticated using TLS 1.3 with self-signed certificates and fingerprint pinning.

**Security Architecture:**
- SSH is used for authentication and rcpd deployment
- Each party generates an ephemeral self-signed certificate
- rcpd outputs its certificate fingerprint to stdout (read by master via SSH)
- Master distributes fingerprints to source/destination for mutual TLS authentication
- All TCP connections use TLS with certificate fingerprint verification

**Security Properties:**
- **Confidentiality**: All data encrypted with AES-256-GCM or ChaCha20-Poly1305
- **Authentication**: Certificate fingerprint verification prevents unauthorized connections
- **Forward secrecy**: TLS 1.3 ephemeral key exchange
- **Integrity**: AEAD ensures data cannot be tampered with

**Opt-out:**
- Use `--no-encryption` flag for trusted networks where performance is critical
- See [security.md](security.md) for detailed threat model and best practices

## 2. Protocol Messages

### 2.1 Handshake Messages

**`TracingHello`** (rcpd → Master, first message on control connection)
- **Purpose**: Identify the role of the connecting rcpd
- **Fields**: `role: RcpdRole` (Source or Destination)
- **Timing**: First message sent after connecting to master

**`MasterHello`** (Master → rcpd, bidirectional stream)
- **Purpose**: Provide configuration and connection details
- **Variants**:
  - `Source { src, dst, dest_cert_fingerprint, filter, dry_run }`: Tells source rcpd what to copy
    - `filter`: Optional filter settings for include/exclude patterns (source-side filtering reduces network traffic)
    - `dry_run`: Optional dry-run mode (brief, all, or explain) for previewing operations without transferring files
  - `Destination { source_control_addr, source_data_addr, server_name, preserve, source_cert_fingerprint }`: Tells destination where to connect (both control and data addresses). Note: empty directory cleanup decisions are communicated per-directory via `keep_if_empty` in `Directory` messages rather than a global flag.

**`SourceMasterHello`** (Source → Master, bidirectional stream)
- **Purpose**: Provide source's TCP server details for destination to connect
- **Fields**: `control_addr`, `data_addr`, `server_name`

**`RcpdResult`** (rcpd → Master, bidirectional stream)
- **Purpose**: Report final success/failure status and statistics
- **Variants**:
  - `Success { message, summary }`
  - `Failure { error, summary }`

### 2.2 Source → Destination Messages (Control Stream)

**`Directory`**
- **Purpose**: Create directory, store metadata, and declare entry counts for completion tracking
- **Fields**: `src`, `dst`, `metadata`, `is_root`, `entry_count`, `file_count`, `keep_if_empty`
- **Usage**: Sent during directory tree traversal in depth-first order. Source pre-reads the directory children before sending this message, so `entry_count` and `file_count` are known at send time. Destination creates the directory, stores metadata, and uses the entry counts for completion tracking.
- **`entry_count`**: Total number of child entries (files + directories + symlinks) that will be sent for this directory. Used by DirectoryTracker to know when all children have been processed.
- **`file_count`**: Number of child files in this directory. Sent back to source via `DirectoryCreated` so source knows how many files to send (round-trip mechanism).
- **`keep_if_empty`**: Whether to keep the directory if it ends up empty after filtering. `true` when no filter is active, when it is the root, or when the directory directly matches an include pattern. `false` when the directory was only traversed to look for potential matches and should be removed if it ends up empty on disk.

**`Symlink`**
- **Purpose**: Create symlink with metadata
- **Fields**: `src`, `dst`, `target`, `metadata`, `is_root`
- **Usage**: Sent during directory traversal when symlink encountered

**`DirStructureComplete`**
- **Purpose**: Signal that all directories and symlinks have been sent
- **Fields**: `has_root_item` (bool) - whether a root file/directory/symlink will be sent
- **Usage**: Sent after recursive directory traversal completes. Required before destination can send `DestinationDone`. When `has_root_item` is false (dry-run mode or filtered root), destination marks root as complete immediately.

**`FileSkipped`**
- **Purpose**: Notify destination that a file failed to send
- **Fields**: `src`, `dst`
- **Usage**: Sent when file open fails (before any data connection is used). Counts as a processed entry for the parent directory's completion tracking. Transport failures after connection is established are fatal.

**`SymlinkSkipped`**
- **Purpose**: Notify destination that a symlink failed to read
- **Fields**: `src_dst: {src, dst}`, `is_root`
- **Usage**: Sent when symlink read fails. If `is_root` is true, destination sets `root_complete` to signal root processing is done (even if failed).

### 2.3 Destination → Source Messages (Control Stream)

**`DirectoryCreated`**
- **Purpose**: Confirm directory created, request file transfers
- **Fields**: `src`, `dst`, `file_count`
- **Usage**: Sent after successfully creating directory. The `file_count` field is echoed back from the `Directory` message so source knows how many files to send from this directory. Triggers source to send files.

**`DestinationDone`**
- **Purpose**: Signal destination has finished all operations
- **Usage**: Final message sent by destination. Initiates graceful shutdown.

### 2.4 File Transfer Messages (Data Connections)

**`File`** (Source → Destination, on data connections)
- **Purpose**: File header followed by raw file data
- **Fields**: `src`, `dst`, `size`, `metadata`, `is_root`
- **Format**: Length-delimited serialized header, then raw bytes (exactly `size` bytes)
- **Connection model**: Connections are pooled and reused for multiple files. The `size` field delimits file boundaries within a connection. Destination reads headers in a loop until EOF.

## 3. Error Communication Design

### 3.1 Asymmetric Error Reporting

The protocol uses asymmetric error communication between source and destination:

**Source → Destination: MUST communicate failures**
- Source must notify destination of skipped files (`FileSkipped`) so destination can track entry counts correctly
- Source must notify destination of skipped symlinks (`SymlinkSkipped`) for logging purposes
- Without these notifications, destination would hang waiting for entries that will never arrive
- **Note**: `FileSkipped` is only sent for file open failures. Transport failures (send errors after connection established) are fatal and abort the entire transfer

**Destination → Source: Does NOT communicate failures**
- Destination handles its own failures locally (logging, error flags)
- Source doesn't need to know if destination failed to create a directory, write a file, etc.
- Source continues sending the complete directory structure regardless of destination failures
- This simplifies the protocol and reduces round-trips

### 3.2 Rationale

This asymmetry reflects the producer-consumer relationship:
- **Source is the producer**: It must tell destination what to expect so destination knows when it's done
- **Destination is the consumer**: It processes what it receives and handles its own problems

If destination fails to create a directory:
- It tracks this locally in `failed_directories`
- It does NOT send `DirectoryCreated`, so source won't send files for it
- It skips any descendant directories/symlinks that arrive (checking `failed_directories`)
- Source is unaware and continues sending the full structure

### 3.3 Root Item Failure Invariant

Root items require special handling to prevent protocol hangs:

**Source side:** If metadata reading fails for a root item (directory or symlink), source MUST
return an error rather than silently continuing. Otherwise, no messages would be sent for the
root item, leaving destination waiting forever for `root_complete` to be set.

**Empty source case:** When no root item will be sent (dry-run mode or filtered root item),
source sets `DirStructureComplete { has_root_item: false }`. Destination uses this flag to
immediately mark root as complete, allowing graceful shutdown without waiting for a root message
that will never arrive.

**Destination side:** If a root item fails to process (directory creation fails, symlink creation
fails), destination MUST set `root_complete = true` before continuing. This ensures `is_done()`
can eventually return true and `DestinationDone` can be sent.

## 4. Protocol Flow

### 4.1 Directory Copy Flow

```
Source                              Destination
  |                                      |
  |  (pre-read root: 2 files, 1 symlink, 1 dir = 4 entries)
  |  ---- Directory(root, entries=4,  -> |  Create root, store metadata
  |         files=2, meta) ----------->  |  entries_expected=4
  |  (pre-read child1: 1 file = 1 entry)
  |  ---- Directory(child1, entries=1,-> |  Create child1, store metadata
  |         files=1, meta) ----------->  |  entries_expected=1
  |                                      |  (child1 does NOT count for root yet)
  |  ---- Symlink(root/link, meta) ----> |  Create symlink
  |                                      |  root: entries_processed++ (1/4)
  |  (pre-read child2: 0 entries)        |
  |  ---- Directory(child2, entries=0,-> |  Create child2, entries_expected=0
  |         files=0, meta) ----------->  |  child2 complete → apply metadata
  |                                      |  child2 notifies root: entries_processed++ (2/4)
  |  ---- DirStructureComplete --------> |  Structure complete
  |                                      |
  |  <--- DirectoryCreated(root, fc=2) - |
  |  <--- DirectoryCreated(child1,fc=1)  |
  |                                      |
  |  (send 2 files from root)           |
  |  ~~~~ File(root/f1) ~~~~~~~~~~~~~~~~>|  Write file
  |                                      |  root: entries_processed++ (3/4)
  |  ~~~~ File(root/f2) ~~~~~~~~~~~~~~~~>|  Write file
  |                                      |  root: entries_processed++ (4/4)
  |                                      |  root complete → apply metadata
  |                                      |
  |  (send 1 file from child1)          |
  |  ~~~~ File(child1/f1) ~~~~~~~~~~~~~> |  Write file
  |                                      |  child1: entries_processed++ (1/1)
  |                                      |  child1 complete → apply metadata
  |                                      |  child1 notifies root (already complete, no-op)
  |                                      |
  |                                      |  All directories complete, structure complete
  |  <--- DestinationDone -------------- |  Close send side
  |                                      |
  |  (detect EOF, close send side)       |  (detect EOF, close recv)
  |  (detect EOF, close recv)            |  Close connection
```

### 4.2 Single File Copy

```
Source                              Destination
  |                                      |
  |  ---- DirStructureComplete --------> |  (no directories)
  |                                      |
  |  ~~~~ File(f, is_root=true) ~~~~~~-> |  (dest opens data conn, receives file)
  |                                      |  Root file complete
  |                                      |
  |  <--- DestinationDone -------------- |  Close send side
  |                                      |
  |  (close send side)                   |
  |  (close connection)                  |
```

### 4.3 Single Symlink Copy

```
Source                              Destination
  |                                      |
  |  ---- Symlink(s, is_root=true) ----> |  Create symlink
  |  ---- DirStructureComplete --------> |  Structure complete, root symlink done
  |                                      |
  |  <--- DestinationDone -------------- |  Close send side
  |                                      |
  |  (close send side)                   |
  |  (close connection)                  |
```

### 4.4 Failed Directory Handling

When a directory fails to be created, destination tracks it locally and skips descendants.
The parent directory's entry count still includes failed children, so `process_child_entry`
is called even for skipped entries to ensure the parent can complete.

```
Source                              Destination
  |                                      |
  |  ---- Directory(dir1, entries=2,  -> |  mkdir dir1 → FAILS
  |         files=1, meta) ----------->  |  Add to failed_directories
  |                                      |  DO NOT send DirectoryCreated
  |                                      |
  |  ---- Directory(dir1/dir2, ...) ---> |  Ancestor failed, skip (log warning)
  |                                      |  parent(dir1) process_child_entry (skipped)
  |  ---- Symlink(dir1/link, meta) ----> |  Ancestor failed, skip (log warning)
  |                                      |  parent(dir1) process_child_entry (skipped)
  |  ---- DirStructureComplete --------> |  Structure complete
  |                                      |
  |  (no DirectoryCreated for dir1)      |
  |  (source doesn't send files)         |
  |                                      |  All pending directories complete
  |  <--- DestinationDone -------------- |
```

## 5. DirectoryTracker

The `DirectoryTracker` on the destination side manages completion state using unified
entry counting. Every child entry (file, directory, or symlink) counts toward the parent
directory's completion, ensuring metadata is only applied after all children finish.

### 5.1 Data Structures

```rust
struct DirectoryTracker {
    /// Directories waiting for entries (entries_expected known, entries_processed < entries_expected)
    pending_directories: HashMap<PathBuf, DirectoryState>,

    /// Directories that failed to create - their descendants are skipped
    failed_directories: HashSet<PathBuf>,

    /// Stored metadata for each directory (applied when complete)
    metadata: HashMap<PathBuf, Metadata>,

    /// Have we received DirStructureComplete?
    structure_complete: bool,

    /// Is the root item complete?
    root_complete: bool,
}

struct DirectoryState {
    entries_expected: usize,    // set from Directory message's entry_count
    entries_processed: usize,   // incremented for each child (file, dir, symlink)
    keep_if_empty: bool,        // whether to keep directory if it has no content
}
```

### 5.2 Completion Conditions

**Directory is complete when:**
- `entries_processed >= entries_expected` (all children processed)

Note: `entries_processed` may exceed `entries_expected` when directory contents change
during the copy (see Section 7.1 for handling of source modifications).

**Root is complete (`DestinationDone` can be sent) when:**
- `structure_complete == true` (all directories/symlinks sent)
- `pending_directories.is_empty()` (all directories complete)
- `root_complete == true` (root item processed)

### 5.3 Key Operations

**On `Directory` message:**
- If ancestor in `failed_directories`: skip, log warning; call `process_child_entry(parent)` to count this entry (directory won't have children to process)
- Try to create directory (see directory creation semantics below)
- If success: add to `pending_directories` with `entries_expected` from message, `entries_processed = 0`, store metadata, send `DirectoryCreated { file_count }` back to source. Do NOT notify parent yet — parent is notified when this directory completes (via `complete_directory`), ensuring bottom-up completion order.
- If failure: add to `failed_directories`; if `is_root`, set `root_complete = true` to avoid hang; if not root, call `process_child_entry(parent)` (directory won't go through `complete_directory`)

**Directory creation semantics:**
- If directory doesn't exist: create it
- If directory already exists: reuse it (success, no `--overwrite` needed)
- If something else exists (file, symlink) and `--overwrite`: remove it and create directory
- If something else exists and no `--overwrite`: fail

This means existing directories are always reusable - the `--overwrite` flag only controls
whether non-directory items can be replaced.

**On `File` message:**
- If `is_root`: write file, set `root_complete = true`
- Otherwise: call `process_file(parent)` which increments `entries_processed`
- If `entries_processed >= entries_expected`: apply stored metadata, remove from `pending_directories`

**On `FileSkipped` message:**
- Call `process_file(parent)` which increments `entries_processed`
- If `entries_processed >= entries_expected`: apply stored metadata, remove from `pending_directories`

**On `Symlink` message:**
- If ancestor in `failed_directories`: skip, log warning; call `process_child_entry(parent)` to count this entry
- Create symlink; if `is_root`, set `root_complete = true` (regardless of success/failure)
- If not root: call `process_child_entry(parent)` to count this symlink

**On `SymlinkSkipped` message:**
- If `is_root`: set `root_complete = true` to avoid hang
- If not root: call `process_child_entry(parent)` to count this entry
- Log the skip

**On `DirStructureComplete { has_root_item }`:**
- Set `structure_complete = true`
- If `has_root_item` is false: set `root_complete = true` (no root messages will follow)
- Check if ready to send `DestinationDone`

## 6. Connection Lifecycle

### 6.1 Shutdown Sequence

The shutdown is coordinated through TCP connection closure:

```
Destination                          Source
  |                                      |
  | All complete, send DestinationDone   |
  | Close control send stream            |
  |                                      |
  |                   (receive DestinationDone)
  |                   (detect send stream EOF)
  |                   Close control send stream
  |                                      |
  | (detect send stream EOF)             |
  | Close control recv stream            |
  | Close TCP connection                 |
  |                                      |
  |                   (detect connection close)
  |                   Close TCP listeners
```

**Key Points:**
- Destination initiates shutdown by sending `DestinationDone` and closing its send side
- Source detects this (EOF on recv), closes its send side
- Destination detects source's send side closed, closes connection
- Both sides close cleanly without needing explicit `SourceDone` message

### 6.2 Connection Types and Ownership

**Control Connection (Bidirectional TCP)**
- **Owner**: Destination connects to source's control port
- **Lifetime**: Entire copy operation
- **Usage**:
  - Source → Destination: Directory/symlink metadata, skip notifications, structure complete
  - Destination → Source: Directory confirmations, done signal

**Data Connections (Pooled TCP)**
- **Model**: Destination opens pool of connections to source's data port; source accepts and sends multiple files per connection
- **Lifetime**: Entire copy operation (reused for multiple files)
- **Usage**: Length-prefixed file headers + raw data (size from header determines bytes to read per file)
- **Pool size**: Controlled by `--max-connections` (default: 100)

### 6.3 Process Termination

**Master Orchestrates Shutdown:**
1. Receives `RcpdResult` from both source and destination
2. Closes TCP connections to both rcpd processes
3. Waits for rcpd SSH processes to exit
4. Reports combined results to user

**rcpd Lifecycle Management:**
- **stdin watchdog**: Monitors stdin for EOF to detect master disconnection
- If master dies unexpectedly, rcpd detects EOF and exits immediately
- No orphaned processes remain on remote hosts

## 7. Design Rationale

### 7.1 Unified Entry Counting and Round-Trip File Count

The protocol uses a two-layer counting scheme for directory completion:

**Entry count (traversal-time, source → destination):**
The `entry_count` in the `Directory` message counts all child entries (files + directories
+ symlinks) visible during source's pre-read of the directory. This count is set at
traversal time and used by DirectoryTracker to determine when all children have been
processed. Since directories, symlinks, and files all count, a parent directory only
completes after all its children are done — preventing premature metadata application.

**File count (round-trip, source → destination → source):**
The `file_count` in the `Directory` message tells destination how many files exist in this
directory. Destination echoes it back in `DirectoryCreated { file_count }` so that source
knows how many files to send. This round-trip mechanism decouples the traversal from file
sending — source pre-reads children during traversal but only sends files after receiving
the `DirectoryCreated` confirmation.

**Handling source modifications during copy:**
Directory contents may change between the source's pre-read (during traversal) and the
actual file sending (after receiving `DirectoryCreated`):

- **Files disappeared:** source sends synthetic `FileSkipped` for missing files, so
  destination's `entries_processed` still reaches `entries_expected`
- **Extra files appeared:** source ignores them (only sends up to `file_count`), logs warning
- **Extra directories/symlinks appeared:** source ignores them (already sent during traversal)
- **Directory unreadable at send time:** source sends `file_count` synthetic `FileSkipped`
  messages so destination can still complete
- With `--fail-early`: abort on any discrepancy

The destination uses `>=` comparison (`entries_processed >= entries_expected`) rather than
`==` to handle edge cases gracefully — if extra entries are somehow processed, the
directory still completes rather than hanging.

### 7.2 Root Item Handling

Root items (the initial copy target) use the `is_root` flag:
- Root items do not decrement any parent directory counter
- Root items trigger `root_complete = true` directly
- This avoids special-case path handling for items without parents

### 7.3 Failed Directory Tracking

Failed directories are tracked in a simple set:
- No entry counting needed for failed directories themselves
- Descendants are detected via ancestor lookup and skipped
- Skipped descendants still call `process_child_entry(parent)` so the parent's entry count
  is correctly maintained — even when a child directory fails, it counts as a processed entry
- Failed directories are not added to `pending_directories` since no `DirectoryCreated` is sent

### 7.4 Message Batching

The protocol uses two sending primitives:

**`send_batch_message()`:** Serializes without flushing.
- Used for: Directories, symlinks during traversal
- Benefit: Multiple messages batched in single network packet

**`send_control_message()`:** Serializes and flushes.
- Used for: `DirStructureComplete`, `DestinationDone`, `DirectoryCreated`
- Critical for correctness at synchronization points

### 7.5 Data Connection Pooling

Data connections are pooled for efficiency:
- Pool size defaults to 100 connections (configurable via `--max-connections`)
- Destination opens connections to source's data port up to pool size
- Source accepts connections and assigns files to them round-robin
- Tasks borrow connections, send files, return them via RAII
- `size` field in headers delimits file boundaries within a connection
- Avoids connection creation overhead per file

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

| State | Cause | Recovery |
|-------|-------|----------|
| **NeedsDrain** | Error before reading data (e.g., can't create file) | Drain `size` bytes, continue with next file |
| **DataConsumed** | Error after reading all data (e.g., metadata failure) | Stream at clean boundary, continue immediately |
| **Corrupted** | Error during data transfer | Close connection (other pooled connections unaffected) |

This distinction matters for pool efficiency:
- `NeedsDrain`: Connection recoverable by draining, pool benefits preserved
- `DataConsumed`: Connection already at clean boundary, can read next header immediately
- `Corrupted`: Connection unusable, must close (source will accept new connection if needed)

### 7.7 Summary Statistics Authority

The destination is the authoritative source for operation statistics:
- Destination knows what actually landed on disk
- Source may send files that destination skips or fails to write
- Master uses only destination's summary for user reporting

### 7.8 Backpressure

The source implements backpressure to prevent unbounded resource usage when the destination
is slower than the source (slow disk, congested network, etc.).

**Problem without backpressure:**
- Source spawns file-sending tasks for all files in a directory
- Each task opens files and allocates buffers while waiting for a connection
- With large directories, this leads to unbounded memory and file descriptor usage

**Solution:**

Two mechanisms work together:

1. **Pending task limit**: A semaphore limits the total number of file-sending tasks that
   can be active at once. Default is `max_connections × 4` (configurable via
   `--pending-writes-multiplier`). Tasks wait on this semaphore before being spawned.

2. **Deferred resource acquisition**: Files are opened and buffers allocated only *after*
   borrowing a connection from the pool. This ensures resources are only held when data
   can actually flow.

**Resource acquisition order:**
```
1. Acquire pending task permit     ← Blocks if too many tasks queued
2. Borrow connection from pool     ← Blocks if all connections busy
3. Open file                       ← Only after connection available
4. Allocate buffer                 ← Only after file opened
5. Send data
6. Release connection + permit
```

**Effect with defaults (100 connections, 4× multiplier):**
- Maximum 400 pending tasks at any time
- Maximum 100 open files (only tasks with connections)
- Maximum ~1.6 GiB buffer memory (100 × 16 MiB)

**Configuration:**
- `--max-connections=N`: Maximum concurrent data connections (default: 100)
- `--pending-writes-multiplier=N`: Multiplier for pending tasks (default: 4)

The multiplier ensures work is always queued when connections become available, avoiding
idle time between file transfers.

## 8. Test Coverage

### 8.1 Core Functionality Tests (`remote_tests.rs`)

**Root item handling:**
- ✅ Single file copy
- ✅ Single symlink copy
- ✅ Directory copy with files

**Complex scenarios:**
- ✅ Nested directories with mixed content
- ✅ Symlink chains and dereferencing
- ✅ Metadata preservation
- ✅ Overwrite scenarios

**Error handling:**
- ✅ Unreadable source files with continue
- ✅ Unreadable source files with fail-early
- ✅ Nested unreadable files
- ✅ Mixed success/failure
- ✅ All operations fail
- ✅ Unwritable destination
- ✅ Root directory blocked by existing file (no hang)
- ✅ Root symlink inaccessible/metadata failure (no hang)

**Lifecycle management:**
- ✅ rcpd exit when master killed
- ✅ No zombie processes
- ✅ Custom connection timeouts

### 8.2 Multi-Host Tests (`docker_multi_host*.rs`)

- ✅ Basic multi-host copy between containers
- ✅ Overwrite protection across hosts
- ✅ Directory copy across hosts
- ✅ Role assignment and logging

### 8.3 Edge Cases

- ✅ Empty directories
- ✅ Deep nesting (100+ levels)
- ✅ Empty files
- ✅ Broken symlinks
- ✅ Circular symlinks

## 9. TCP Configuration

### 9.1 Connection Settings

Both `rcp` and `rcpd` accept CLI arguments for TCP connection behavior:

- `--remote-copy-conn-timeout-sec=N` (default: 15) - Connection timeout for remote operations
- `--port-ranges=RANGES` (optional) - Restrict TCP to specific port ranges (e.g., "8000-8999")
- `--max-connections=N` (default: 100) - Maximum concurrent data connections
- `--pending-writes-multiplier=N` (default: 4) - Multiplier for pending file tasks (backpressure)
- `--network-profile=PROFILE` (default: datacenter) - Buffer sizing profile

### 9.2 Network Profiles

**Datacenter Profile (default):**
- Larger TCP buffer sizes (16 MiB)
- Optimized for low-latency, high-bandwidth networks

**Internet Profile:**
- Smaller TCP buffer sizes (2 MiB)
- More conservative settings for higher-latency networks

### 9.3 Tuning Guidelines

- **Datacenter**: Use default settings for best performance
- **Internet/WAN**: Use `--network-profile=internet` for better behavior on higher-latency links
- **Firewall-restricted**: Use `--port-ranges` to specify allowed ports
