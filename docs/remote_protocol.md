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
2. Master creates a QUIC server endpoint and waits for connections
3. Master spawns source rcpd via SSH: `ssh user@host1 rcpd --master-addr=... --role=source ...`
4. Master spawns destination rcpd via SSH: `ssh user@host2 rcpd --master-addr=... --role=destination ...`
5. Both rcpd processes connect to master via QUIC
6. Master identifies them by their declared role (sent via `TracingHello`)

**Special Case - Same Host Copies:**
When source and destination are on the same host, the master:
- Deploys rcpd only once (if needed)
- Starts two separate rcpd processes with different roles
- Both processes share the same SSH session but have distinct connections

### 1.3 Connection Topology

The system uses a **triangle topology** with three QUIC connections:

```
         Master (rcp)
           /    \
          /      \
    (QUIC)      (QUIC)
        /          \
       /            \
Source (rcpd)-----(QUIC)----Destination (rcpd)
```

**Connection Details:**

1. **Source → Master**: Bidirectional, used for handshake and result reporting
2. **Destination → Master**: Bidirectional, used for handshake and result reporting
3. **Destination → Source**: Bidirectional + multiple unidirectional
   - Control stream (bidirectional): Directory metadata and coordination
   - File streams (unidirectional): One per file transfer

**Connection Establishment Order:**

1. Both rcpd processes connect to master (order undefined)
2. Master sends `MasterHello::Source` to source rcpd with src/dst paths
3. Source rcpd starts QUIC server, sends `SourceMasterHello` back to master with address and certificate fingerprint
4. Master sends `MasterHello::Destination` to destination rcpd with source address and certificate
5. Destination rcpd connects to source rcpd

### 1.4 Certificate Pinning

All connections use certificate pinning to prevent MITM attacks:

- **Master → rcpd**: rcpd validates master's certificate using `master_cert_fingerprint`
- **Destination → Source**: Destination validates source's certificate using `source_cert_fingerprint` provided by master

## 2. Protocol Messages

### 2.1 Handshake Messages

**`TracingHello`** (rcpd → Master, unidirectional stream)
- **Purpose**: Identify the role of the connecting rcpd
- **Fields**: `role: RcpdRole` (Source or Destination)
- **Timing**: First message sent after connecting to master

**`MasterHello`** (Master → rcpd, bidirectional stream)
- **Purpose**: Provide configuration and connection details
- **Variants**:
  - `Source { src, dst }`: Tells source rcpd what to copy
  - `Destination { source_addr, server_name, source_cert_fingerprint, preserve }`: Tells destination where to connect

**`SourceMasterHello`** (Source → Master, bidirectional stream)
- **Purpose**: Provide source's QUIC server details for destination to connect
- **Fields**: `source_addr`, `server_name`, `cert_fingerprint`

**`RcpdResult`** (rcpd → Master, bidirectional stream)
- **Purpose**: Report final success/failure status and statistics
- **Variants**:
  - `Success { message, summary }`
  - `Failure { error, summary }`

### 2.2 Source → Destination Messages (Control Stream)

**`Directory`**
- **Purpose**: Create directory and store metadata for later application
- **Fields**: `src`, `dst`, `metadata`, `is_root`
- **Usage**: Sent during directory tree traversal in depth-first order. Destination creates the directory and stores metadata to apply when all files are received.

**`Symlink`**
- **Purpose**: Create symlink with metadata
- **Fields**: `src`, `dst`, `target`, `metadata`, `is_root`
- **Usage**: Sent during directory traversal when symlink encountered

**`DirStructureComplete`**
- **Purpose**: Signal that all directories and symlinks have been sent
- **Usage**: Sent after recursive directory traversal completes. Required before destination can send `DestinationDone`.

**`FileSkipped`**
- **Purpose**: Notify destination that a file failed to send
- **Fields**: `src`, `dst`, `dir_total_files`
- **Usage**: Sent when file open/read fails. Allows destination to track file counts correctly.

**`SymlinkSkipped`**
- **Purpose**: Notify destination that a symlink failed to read
- **Fields**: `src_dst: {src, dst}`, `is_root`
- **Usage**: Sent when symlink read fails. If `is_root` is true, destination sets `root_complete` to signal root processing is done (even if failed).

**`DirectoryEmpty`**
- **Purpose**: Notify destination that a directory contains no files
- **Fields**: `src`, `dst`
- **Usage**: Sent after receiving `DirectoryCreated` for an empty directory. Allows destination to mark directory as complete.

### 2.3 Destination → Source Messages (Control Stream)

**`DirectoryCreated`**
- **Purpose**: Confirm directory created, request file transfers
- **Fields**: `src`, `dst`
- **Usage**: Sent after successfully creating directory. Triggers source to send files from this directory.

**`DestinationDone`**
- **Purpose**: Signal destination has finished all operations
- **Usage**: Final message sent by destination. Initiates graceful shutdown.

### 2.4 File Transfer Messages (Unidirectional Streams)

**`File`** (Source → Destination)
- **Purpose**: File header followed by raw file data
- **Fields**: `src`, `dst`, `size`, `metadata`, `is_root`, `dir_total_files`
- **Format**: Serialized header, then raw bytes (exactly `size` bytes)
- **Stream**: Multiple files share pooled streams; the `size` field delimits file boundaries

The `dir_total_files` field tells destination how many files to expect for this file's parent directory. This count is set when source iterates the directory contents (after receiving `DirectoryCreated`), ensuring accuracy even if directory contents change during the copy.

## 3. Error Communication Design

### 3.1 Asymmetric Error Reporting

The protocol uses asymmetric error communication between source and destination:

**Source → Destination: MUST communicate failures**
- Source must notify destination of skipped files (`FileSkipped`) so destination can track file counts correctly
- Source must notify destination of skipped symlinks (`SymlinkSkipped`) for logging purposes
- Without these notifications, destination would hang waiting for items that will never arrive

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
root item, but `DirStructureComplete` would still be sent, leaving destination waiting forever
for `root_complete` to be set.

**Destination side:** If a root item fails to process (directory creation fails, symlink creation
fails), destination MUST set `root_complete = true` before continuing. This ensures `is_done()`
can eventually return true and `DestinationDone` can be sent.

## 4. Protocol Flow

### 4.1 Directory Copy Flow

```
Source                              Destination
  |                                      |
  |  ---- Directory(root, meta) -------> |  Create root, store metadata
  |  ---- Directory(child1, meta) -----> |  Create child1, store metadata
  |  ---- Symlink(root/link, meta) ----> |  Create symlink
  |  ---- Directory(child2, meta) -----> |  Create child2, store metadata
  |  ---- DirStructureComplete --------> |  Structure complete
  |                                      |
  |  <--- DirectoryCreated(root) ------- |
  |  <--- DirectoryCreated(child1) ----- |
  |  <--- DirectoryCreated(child2) ----- |
  |                                      |
  |  (iterate root, find 2 files)        |
  |  ~~~~ File(root/f1, total=2) ~~~~~~> |  Write file, track: 2 expected, 1 remaining
  |  ~~~~ File(root/f2, total=2) ~~~~~~> |  Write file, 0 remaining
  |                                      |  root complete → apply metadata
  |                                      |
  |  (iterate child1, find 1 file)       |
  |  ~~~~ File(child1/f1, total=1) ~~~-> |  Write file, 0 remaining
  |                                      |  child1 complete → apply metadata
  |                                      |
  |  (iterate child2, empty)             |
  |  ---- DirectoryEmpty(child2) ------> |  0 files expected
  |                                      |  child2 complete → apply metadata
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
  |  ~~~~ File(f, is_root=true) ~~~~~~-> |  Write file
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

When a directory fails to be created, destination tracks it locally and skips descendants:

```
Source                              Destination
  |                                      |
  |  ---- Directory(dir1, meta) -------> |  mkdir dir1 → FAILS
  |                                      |  Add to failed_directories
  |                                      |  Set files_expected = 0 (no files will come)
  |                                      |  DO NOT send DirectoryCreated
  |                                      |
  |  ---- Directory(dir1/dir2, meta) --> |  Ancestor failed, skip (log warning)
  |  ---- Symlink(dir1/link, meta) ----> |  Ancestor failed, skip (log warning)
  |  ---- DirStructureComplete --------> |  Structure complete
  |                                      |
  |  (no DirectoryCreated for dir1)      |
  |  (source doesn't send files)         |
  |                                      |  All pending directories complete
  |  <--- DestinationDone -------------- |
```

## 5. DirectoryTracker

The `DirectoryTracker` on the destination side manages completion state with minimal tracking:

### 5.1 Data Structures

```rust
struct DirectoryTracker {
    /// Directories waiting for files (files_expected unknown or files_remaining > 0)
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
    files_expected: Option<usize>,  // None until first File/FileSkipped/DirectoryEmpty
    files_remaining: usize,
}
```

### 5.2 Completion Conditions

**Directory is complete when:**
- `files_expected.is_some()` (we know how many files to expect)
- `files_remaining == 0` (all files received)

**Root is complete (`DestinationDone` can be sent) when:**
- `structure_complete == true` (all directories/symlinks sent)
- `pending_directories.is_empty()` (all directories complete)
- `root_complete == true` (root item processed)

### 5.3 Key Operations

**On `Directory` message:**
- If ancestor in `failed_directories`: skip, log warning
- Try to create directory (see directory creation semantics below)
- If success: add to `pending_directories` with `files_expected = None`, store metadata, send `DirectoryCreated`
- If failure: add to `failed_directories`; if `is_root`, set `root_complete = true` to avoid hang

**Directory creation semantics:**
- If directory doesn't exist: create it
- If directory already exists: reuse it (success, no `--overwrite` needed)
- If something else exists (file, symlink) and `--overwrite`: remove it and create directory
- If something else exists and no `--overwrite`: fail

This means existing directories are always reusable - the `--overwrite` flag only controls
whether non-directory items can be replaced.

**On `File` message:**
- If `is_root`: write file, set `root_complete = true`
- Otherwise: set `files_expected` from `dir_total_files`, decrement `files_remaining`
- If `files_remaining == 0`: apply stored metadata, remove from `pending_directories`

**On `FileSkipped` message:**
- Set `files_expected` from `dir_total_files`, decrement `files_remaining`
- If `files_remaining == 0`: apply stored metadata, remove from `pending_directories`

**On `DirectoryEmpty` message:**
- Set `files_expected = Some(0)`, `files_remaining = 0`
- Apply stored metadata, remove from `pending_directories`

**On `Symlink` message:**
- If ancestor in `failed_directories`: skip, log warning
- Create symlink; if `is_root`, set `root_complete = true` (regardless of success/failure)
- Non-root symlinks don't affect tracking

**On `SymlinkSkipped` message:**
- If `is_root`: set `root_complete = true` to avoid hang
- Otherwise: log only (symlinks don't affect file counts)

**On `DirStructureComplete`:**
- Set `structure_complete = true`
- Check if ready to send `DestinationDone`

## 6. Connection Lifecycle

### 6.1 Shutdown Sequence

The shutdown is coordinated through QUIC stream closure:

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
  | Close QUIC connection                |
  |                                      |
  |                   (detect connection close)
  |                   Close QUIC endpoint
```

**Key Points:**
- Destination initiates shutdown by sending `DestinationDone` and closing its send side
- Source detects this (EOF on recv), closes its send side
- Destination detects source's send side closed, closes connection
- Both sides close cleanly without needing explicit `SourceDone` message

### 6.2 Stream Types and Ownership

**Control Stream (Bidirectional)**
- **Owner**: Source opens, destination accepts
- **Lifetime**: Entire copy operation
- **Usage**:
  - Source → Destination: Directory/symlink metadata, skip notifications, structure complete
  - Destination → Source: Directory confirmations, done signal

**File Streams (Unidirectional, Pooled)**
- **Owner**: Source creates a pool of streams at connection establishment
- **Lifetime**: Pool lifetime matches the connection; streams are reused for multiple files
- **Usage**: File header + raw data (size from header determines bytes to read)
- **Pool behavior**: Tasks borrow streams from the pool, send files, and return them automatically

### 6.3 Process Termination

**Master Orchestrates Shutdown:**
1. Receives `RcpdResult` from both source and destination
2. Closes QUIC connections to both rcpd processes
3. Waits for QUIC endpoint to idle (with 500ms timeout)
4. Waits for rcpd SSH processes to exit
5. Reports combined results to user

**rcpd Lifecycle Management:**
- **stdin watchdog**: Monitors stdin for EOF to detect master disconnection
- **QUIC idle timeout**: Secondary mechanism if stdin unavailable
- If master dies unexpectedly, rcpd detects EOF and exits immediately
- No orphaned processes remain on remote hosts

## 7. Design Rationale

### 7.1 Deferred File Count

The protocol defers communicating file counts until after the source iterates directory contents:

**Problem:** Directory contents may change between initial traversal and file sending.

**Solution:** The `dir_total_files` field is set when source actually iterates the directory (after receiving `DirectoryCreated`), not during initial traversal.

**Handling changes:**
- If fewer files exist than during iteration: source sends `FileSkipped` for missing files
- If more files exist than during iteration: source ignores extras, logs warning
- With `--fail-early`: abort on any discrepancy

This guarantees `dir_total_files` matches exactly what source sends, preventing hangs.

### 7.2 Root Item Handling

Root items (the initial copy target) use the `is_root` flag:
- Root items do not decrement any parent directory counter
- Root items trigger `root_complete = true` directly
- This avoids special-case path handling for items without parents

### 7.3 Failed Directory Tracking

Failed directories are tracked in a simple set:
- No complex entry counting needed for failed directories
- Descendants are detected via ancestor lookup and skipped
- Since we only count files (not subdirs/symlinks), skipping descendants doesn't affect counts
- Failed directories immediately have `files_expected = Some(0)` since no `DirectoryCreated` is sent

### 7.4 Message Batching

The protocol uses two sending primitives:

**`send_batch_message()`:** Serializes without flushing.
- Used for: Directories, symlinks during traversal
- Benefit: Multiple messages batched in single network packet

**`send_control_message()`:** Serializes and flushes.
- Used for: `DirStructureComplete`, `DestinationDone`, `DirectoryCreated`
- Critical for correctness at synchronization points

### 7.5 Stream Pooling

File streams are pooled for efficiency:
- Pool size defaults to 100 streams (configurable via `--quic-max-concurrent-streams`)
- Tasks borrow streams, send files, return them via RAII
- `size` field in headers delimits file boundaries within a stream
- Avoids stream creation overhead per file

### 7.6 Stream Error Recovery

When processing a file fails, the destination must determine if the stream can continue
receiving more files. There are three possible states:

| State | Cause | Stream Position | Recovery |
|-------|-------|-----------------|----------|
| **NeedsDrain** | Error before reading data (e.g., can't create file) | At start of file data | Drain `size` bytes, continue |
| **DataConsumed** | Error after reading all data (e.g., metadata failure) | At next file header | Continue immediately |
| **Corrupted** | Error during data transfer | Unknown | Close stream |

This distinction matters for stream pooling efficiency:
- `NeedsDrain`: Stream recoverable by draining, pool benefits preserved
- `DataConsumed`: Stream already at clean boundary, can read next header immediately
- `Corrupted`: Stream unusable, must close (other pooled streams unaffected)

Previously, `DataConsumed` errors (like metadata failures) were treated as corrupted,
unnecessarily closing streams that could continue serving files.

**Test coverage:** `test_remote_sudo_stream_continues_after_metadata_error` (requires passwordless sudo)

### 7.7 Summary Statistics Authority

The destination is the authoritative source for operation statistics:
- Destination knows what actually landed on disk
- Source may send files that destination skips or fails to write
- Master uses only destination's summary for user reporting

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
- ✅ Custom QUIC timeouts

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

## 9. QUIC Configuration

### 9.1 Timeout Configuration

Both `rcp` and `rcpd` accept CLI arguments for QUIC connection behavior:

- `--quic-idle-timeout-sec=N` (default: 10) - Maximum idle time before closing connection
- `--quic-keep-alive-interval-sec=N` (default: 1) - Interval for keep-alive packets
- `--remote-copy-conn-timeout-sec=N` (default: 15) - Connection timeout for remote operations

### 9.2 Tuning Guidelines

- **Datacenter**: More aggressive values (5-8s idle timeout) for faster failure detection
- **Internet**: Higher values (15-30s idle timeout) to handle network hiccups
- **High latency**: Increase all timeouts proportionally
