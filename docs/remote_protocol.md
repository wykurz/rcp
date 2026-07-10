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
2. Master spawns source rcpd via SSH: `ssh user@host1 rcpd --role=source --master-cert-fp=... ...`
3. Master spawns destination rcpd via SSH: `ssh user@host2 rcpd --role=destination --master-cert-fp=... ...`
4. Each rcpd creates a TCP listener and prints `RCP_TLS <addr> <fingerprint>` (or
   `RCP_TCP <addr>` when encryption is disabled) to stderr; master reads that line via SSH
5. Master connects to each rcpd's listener via TCP: first the control connection, then a
   second connection used for tracing/progress forwarding

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

1. **Master → Source**: Bidirectional TCP (master connects to source rcpd's listener),
   used for handshake and result reporting
2. **Master → Destination**: Bidirectional TCP (master connects to destination rcpd's
   listener), used for handshake and result reporting
3. **Source ↔ Destination**: Two TCP ports on source (destination connects to both):
   - **Control port**: Bidirectional TCP for directory metadata and coordination
   - **Data port**: A pool of TCP connections for file transfers (each connection is
     reused for multiple files)

**Connection Establishment Order:**

1. Master connects to both rcpd processes (each rcpd listens; master learns the address
   from the `RCP_TLS`/`RCP_TCP` line on the rcpd's stderr)
2. Master sends `MasterHello::Source` to source rcpd with src/dst paths
3. Source rcpd starts TCP listeners (control + data), sends `SourceMasterHello` back to master with both addresses
4. Master sends `MasterHello::Destination` to destination rcpd with source addresses
5. Destination rcpd connects to source's control port
6. Destination opens a pool of connections to source's data port; files are streamed over
   these pooled connections (the `size` field in each header delimits file boundaries)

### 1.4 Security Model

All TCP connections are encrypted and authenticated using TLS 1.3 with self-signed
certificates and fingerprint pinning. TLS 1.3 is pinned in the config (TLS 1.2 is never
negotiated) — see the [Cipher Suites](security.md#cipher-suites) section of security.md.

**Security Architecture:**
- SSH is used for authentication and rcpd deployment
- Each party generates an ephemeral self-signed certificate
- rcpd outputs its certificate fingerprint to stderr (read by master via SSH)
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
  - `Success { message, summary, runtime_stats }`
  - `Failure { error, summary, runtime_stats }`

### 2.2 Source → Destination Messages (Control Stream)

**`Directory`**
- **Purpose**: Create directory, store metadata, and declare the entry count for completion tracking
- **Fields**: `src`, `dst`, `metadata`, `is_root`, `entry_count`, `keep_if_empty`
- **Usage**: Sent during directory tree traversal in depth-first order. Source pre-reads the directory children before sending this message, so `entry_count` is known at send time. Destination creates the directory, stores metadata, and uses the entry count for completion tracking.
- **`entry_count`**: Total number of child entries (files + directories + symlinks) that will be sent for this directory. Used by DirectoryTracker to know when all children have been processed.
- **`keep_if_empty`**: Whether to keep the directory if it ends up empty after filtering. `true` when no filter is active, when it is the root, or when the directory directly matches an include pattern. `false` when the directory was only traversed to look for potential matches and should be removed if it ends up empty on disk.
- **No `file_count`**: the source retains the child-file count it computed during the pre-read (in its fd-map entry under hardened reads, or in a path→count map under `-L`), so it is not sent on the wire and not echoed back. See §7.1.

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

**`FileUnchanged`**
- **Purpose**: Notify destination that the source skipped transferring a file because the
  destination already holds a matching entry (per the directory manifest in `DirectoryManifestChunk`s).
- **Fields**: `src`, `dst`
- **Usage**: Sent on the control stream like `FileSkipped`, but signals a *successful* skip (the
  destination copy is already identical) rather than a failure. Counts as a processed entry for
  the parent directory and as `files_unchanged` (the destination is authoritative for that count).
  No file data is sent for the skipped file.

**`SymlinkSkipped`**
- **Purpose**: Notify destination that a symlink failed to read
- **Fields**: `src_dst: {src, dst}`, `is_root`
- **Usage**: Sent when symlink read fails. If `is_root` is true, destination sets `root_complete` to signal root processing is done (even if failed).

### 2.3 Destination → Source Messages (Control Stream)

**`DirectoryManifestChunk`**
- **Purpose**: Carry a chunk of the reused destination directory's pre-existing-entry manifest,
  used by the source to skip transferring identical files.
- **Fields**: `dst`, `entries: Vec<ExistingEntry>` (each `ExistingEntry` carries `name`,
  `is_file`, `metadata`, `size`)
- **Usage**: A directory's manifest is split into one or more chunks, each well under the control
  stream's frame limit (`LengthDelimitedCodec`, 8 MiB), and **all** of them are sent **before**
  that directory's `DirectoryCreated`. The control stream is FIFO, so the source has the complete
  manifest by the time it processes `DirectoryCreated`. No chunks are sent when the directory was
  freshly created (not reused), when neither `--overwrite` nor `--ignore-existing` is active, or
  when the directory's entry count exceeds the manifest cap (`--overwrite-manifest-max-entries`,
  default 5,000,000) — in which case that directory falls back to transferring-and-draining (the
  baseline behavior). Chunking the manifest (rather than inlining it in `DirectoryCreated`)
  ensures the cap stays meaningful without any single control frame exceeding the frame limit.
  See §7.9.

**`DirectoryCreated`**
- **Purpose**: Confirm directory created, request file transfers
- **Fields**: `src`, `dst`
- **Usage**: Sent after successfully creating directory, and after any `DirectoryManifestChunk`s
  for it. This is purely the Pass-2 trigger: it tells the source the destination created the
  directory and is ready to receive its files. The source already retains the authoritative
  child-file count it computed during the Pass-1 pre-read (hardened: in the fd-map entry; `-L`: in
  a path→count map), so no count is echoed back. Triggers source to send files. See §7.1.

**`DirectorySkipped`**
- **Purpose**: Acknowledge a `Directory` message the destination did NOT create (create failed, ancestor failed, or `--ignore-existing` skipped a non-directory), so no files will be requested for it.
- **Fields**: `src`, `dst`
- **Usage**: The destination sends **exactly one** of `DirectoryCreated` / `DirectorySkipped` per `Directory` message. The source uses this to release the directory's held fd in its source-side fd-map (the TOCTOU-safe-read dir-fd budget). Without this nack a skipped directory's fd permit would never be released, so a no-ack subtree larger than the budget would block the source's Pass-1 walk and hang the copy. Does not affect completion accounting: skipped directories were never added to `pending_directories`.

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
- Source must notify destination of skipped-identical files (`FileUnchanged`) so destination can track entry counts correctly — an optimization notification (the destination already matched), not a failure, but it serves the same count-tracking role as `FileSkipped`
- Without these notifications, destination would hang waiting for entries that will never arrive
- **Note**: `FileSkipped` is only sent for file open failures. Transport failures (send errors after connection established) are fatal and abort the entire transfer

**Destination → Source: Does NOT communicate failures (one exception: directory acks)**
- Destination handles its own failures locally (logging, error flags)
- Source continues sending the complete directory structure regardless of destination failures
- This simplifies the protocol and reduces round-trips
- Destination metadata errors (file, directory, symlink) are handled locally: logged with
  `tracing::error!`, the error recorded in the `ErrorCollector`, and processing continues unless `--fail-early`
  is set. This applies to both file metadata (via `DataConsumed` stream state) and directory
  metadata (in `DirectoryTracker::complete_directory_single`)
- **Exception — directory acks:** the destination DOES tell the source the outcome of every
  `Directory` message, sending exactly one of `DirectoryCreated` (success/reuse) or
  `DirectorySkipped` (not created). This is not failure reporting for its own sake — the source
  needs it to release the directory's held fd from its source-side fd-map (TOCTOU-safe reads).
  It does not change what the source sends next (a skipped directory's children still arrive and
  are skipped via `failed_directories`).

### 3.2 Rationale

This asymmetry reflects the producer-consumer relationship:
- **Source is the producer**: It must tell destination what to expect so destination knows when it's done
- **Destination is the consumer**: It processes what it receives and handles its own problems

If destination fails to create a directory:
- It tracks this locally in `failed_directories`
- It sends `DirectorySkipped` (not `DirectoryCreated`), so source won't send files for it but
  does release the directory's held fd (fd-map). The same `DirectorySkipped` is sent when a
  directory is skipped because an ancestor failed, or `--ignore-existing` skips a non-directory.
- It skips any descendant directories/symlinks that arrive (checking `failed_directories`)
- Source continues sending the full structure (it only releases the skipped dir's fd)

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
  |  (retain root file_count=2 source-side)
  |  ---- Directory(root, entries=4,  -> |  Create root, store metadata
  |         meta) -------------------->  |  entries_expected=4
  |  (pre-read child1: 1 file = 1 entry; retain child1 file_count=1)
  |  ---- Directory(child1, entries=1,-> |  Create child1, store metadata
  |         meta) -------------------->  |  entries_expected=1
  |                                      |  (child1 does NOT count for root yet)
  |  ---- Symlink(root/link, meta) ----> |  Create symlink
  |                                      |  root: entries_processed++ (1/4)
  |  (pre-read child2: 0 entries; retain child2 file_count=0)
  |  ---- Directory(child2, entries=0,-> |  Create child2, entries_expected=0
  |         meta) -------------------->  |  child2 complete → apply metadata
  |                                      |  child2 notifies root: entries_processed++ (2/4)
  |  ---- DirStructureComplete --------> |  Structure complete
  |                                      |
  |  <--- DirectoryManifestChunk(root)   |  (0+ chunks for reused dirs under
  |                                      |   --overwrite/--ignore-existing, sent
  |                                      |   BEFORE the trigger; none otherwise)
  |  <--- DirectoryCreated(root) ------- |  (trigger only; no count echoed)
  |  <--- DirectoryCreated(child1) ----- |
  |                                      |
  |  (look up retained file_count=2;     |
  |   compare f1, f2 against manifest)   |
  |  ~~~~ File(root/f1) ~~~~~~~~~~~~~~~> |  Write file (f1 not in manifest or differs)
  |                                      |  root: entries_processed++ (3/4)
  |  ---- FileUnchanged(root/f2) ------> |  f2 matches manifest; no data sent
  |                                      |  root: entries_processed++ (4/4)
  |                                      |  root complete → apply metadata
  |                                      |
  |  (send 1 file from child1)           |
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
  |         meta) -------------------->  |  Add to failed_directories
  |  <--- DirectorySkipped(dir1) ------- |  Nack (not created)
  |  (release dir1 fd, no Pass 2)        |
  |                                      |
  |  ---- Directory(dir1/dir2, ...) ---> |  Ancestor failed, skip (log warning)
  |  <--- DirectorySkipped(dir1/dir2) -- |  Nack; parent(dir1) process_child_entry
  |  (release dir1/dir2 fd, no Pass 2)   |
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

The completion-tracking core, abridged — the full struct in `rcp/src/directory_tracker.rs`
additionally holds the destination-side directory fd-map (`dirs`, plus the root's parent
fd) through which child writes resolve, `created_directories` (for empty-directory
cleanup), the control send stream, preserve/fail-early settings, and an error collector:

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
- If success: add to `pending_directories` with `entries_expected` from message, `entries_processed = 0`, store metadata, send `DirectoryCreated { src, dst }` back to source (the Pass-2 trigger; no count is echoed — the source retains its own file count). Do NOT notify parent yet — parent is notified when this directory completes (via `complete_directory`), ensuring bottom-up completion order.
- If failure: add to `failed_directories`; if `is_root`, set `root_complete = true` to avoid hang; if not root, call `process_child_entry(parent)` (directory won't go through `complete_directory`)

**Directory creation semantics:**
- If directory doesn't exist: create it
- If directory already exists: reuse it (success, no `--overwrite` needed)
- If something else exists (file, symlink) and `--overwrite`: remove it and create directory
- If something else exists and no `--overwrite`: fail

This means existing directories are always reusable - the `--overwrite` flag only controls
whether non-directory items can be replaced.

**On directory completion (`complete_directory_single`):**
- Apply stored metadata (permissions, owner, timestamps)
- If metadata application fails (e.g., `fchownat` EPERM): log error, push the error to the
  `ErrorCollector`, return error only if `fail_early`. Otherwise continue — the directory is
  still marked complete and parent notification still happens.

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

### 7.1 Unified Entry Counting and Source-Retained File Count

The protocol uses a two-layer counting scheme for directory completion:

**Entry count (traversal-time, source → destination):**
The `entry_count` in the `Directory` message counts all child entries (files + directories
+ symlinks) visible during source's pre-read of the directory. This count is set at
traversal time and used by DirectoryTracker to determine when all children have been
processed. Since directories, symlinks, and files all count, a parent directory only
completes after all its children are done — preventing premature metadata application.

**File count (source-retained, no round-trip):**
The number of child files in a directory is computed by the source during the Pass-1
pre-read and **retained on the source side** — it is not sent on the wire and not echoed
back. The destination only signals readiness:

- **Hardened reads (default):** the count is stored in the directory's source-side fd-map
  entry (alongside the held `O_NOFOLLOW` directory fd) keyed by source path.
- **`-L`/`--dereference`:** the source holds no fd-map, so the count is stored in a separate
  `path → file_count` map.

`DirectoryCreated { src, dst }` is purely the **Pass-2 trigger**: when it arrives the source
looks up the directory's retained count and begins sending its files. This still decouples
traversal (Pass 1) from file sending (Pass 2) — the source pre-reads children during
traversal but only sends files after the `DirectoryCreated` confirmation — but without a
file-count round-trip. Under hardened reads a `DirectoryCreated` whose directory has no
retained entry is a TOCTOU-safety / protocol-invariant violation and **fails closed** (the
source refuses to re-resolve the directory by path); under `-L` a missing count defaults to
0 with a debug log (that path is not hardened, so a miss is not a fail-closed condition).

**Committed-but-unreadable directory (tombstone):**
When the source commits a directory to the wire (the `is_dir` pre-check passed) but then
cannot open or enumerate it, it sends a 0-entry `Directory` so the destination creates an
empty directory and completes its tracking. To keep the fail-closed rule from mis-firing on
this legitimate case, the source registers a matching retained entry before sending:

- If the directory fd is held (only enumeration failed): a real 0-file fd-map entry.
- If the directory could not be opened at all: a **tombstone** (no held fd, no fd-budget
  permit, file_count 0).

Either way the destination's `DirectoryCreated` ack consumes a real entry instead of hitting
the fail-closed miss path, and Pass 2 sends zero files. This preserves the
"unreadable directory → continue as an empty directory unless `--fail-early`" behavior for
both root and non-root directories. (A *true* miss — a directory that was never committed,
or whose entry was already consumed — still fails closed.)

**Handling source modifications during copy:**
Directory contents may change between the source's pre-read (during traversal) and the
actual file sending (after receiving `DirectoryCreated`):

- **Files disappeared:** source sends synthetic `FileSkipped` for missing files, so
  destination's `entries_processed` still reaches `entries_expected`
- **Extra files appeared:** source ignores them (only sends up to the retained file count),
  logs warning
- **Extra directories/symlinks appeared:** source ignores them (already sent during traversal)
- **Directory unreadable at send time:** source sends one synthetic `FileSkipped` per retained
  expected file so destination can still complete
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
- Failed directories are not added to `pending_directories`; a `DirectorySkipped` nack is sent in
  place of `DirectoryCreated` (exactly one of the two is sent per `Directory`)

### 7.4 Message Batching

The protocol uses two sending primitives:

**`send_batch_message()`:** Serializes without flushing.
- Used for: Directories, symlinks during traversal
- Benefit: Multiple messages batched in single network packet

**`send_control_message()`:** Serializes and flushes.
- Used for: `DirStructureComplete`, `DestinationDone`, `DirectoryCreated`, `DirectorySkipped`
- Critical for correctness at synchronization points

### 7.5 Data Connection Pooling

Data connections are pooled for efficiency:
- Pool size defaults to 100 connections (configurable via `--max-connections`)
- Destination opens connections to source's data port up to pool size
- Source accepts connections into a shared pool of available send streams; each file-send
  task borrows the next free connection and returns it for reuse (RAII)
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

Directory metadata errors are handled analogously in `DirectoryTracker::complete_directory_single`:
the error is logged, pushed to the `ErrorCollector`, and processing continues (unless `--fail-early`).
The directory is still marked complete and parent notifications still propagate.

### 7.7 Summary Statistics Authority

The master merges source and destination summaries based on mode:

- **Normal mode**: destination is authoritative for copy/create/unchanged/remove counts
  (it knows what actually landed on disk). Source is authoritative for skip counts
  (filtered and special-file skips happen before items reach the destination).
- **Dry-run mode**: source is authoritative for all counts (destination is idle).

Skip counts (`files_skipped`, `symlinks_skipped`, `directories_skipped`,
`specials_skipped`) always come from the source regardless of mode.

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

### 7.9 Skipping identical files (destination manifest + source decision)

When `--overwrite` or `--ignore-existing` is active, the destination can supply the source
with a manifest of the reused directory's pre-existing entries so the source can skip
transferring files that are already up to date.

**Mechanism:**

1. **Pass 1 (directory creation):** When the destination reuses an existing directory and the
   feature is active, it enumerates the directory's children fd-relatively (using the same
   `read_entries()` + `child()` pattern as the source's TOCTOU-safe walk — the directory is
   pinned via an `O_NOFOLLOW` handle and names are never re-resolved by path). The resulting
   manifest (`Vec<ExistingEntry>`) is split into one or more `DirectoryManifestChunk` messages,
   each kept well under the control stream's `LengthDelimitedCodec` frame limit (8 MiB), and sent
   **before** that directory's `DirectoryCreated`. The control stream is FIFO, so the source has
   the complete manifest in hand when it processes `DirectoryCreated`. Chunking keeps the entry
   cap (default 5,000,000) meaningful without ever producing a single oversized control frame.

2. **Pass 2 (file sending):** For each file the source would normally transfer, it looks up the
   file's name in the manifest. If a matching `ExistingEntry` is found, the source applies the
   same comparison logic as the local `--overwrite` path (`--overwrite-compare`, default
   `size,mtime`; `--overwrite-filter=newer` is also honored). Under `--ignore-existing`, any
   name collision causes a skip regardless of entry type. When the comparison determines the
   destination copy is already identical (or should be left alone), the source sends
   `SourceMessage::FileUnchanged { src, dst }` on the control stream instead of opening a data
   connection and transferring the file.

3. **Accounting:** `FileUnchanged` counts as a processed entry for the parent directory's
   completion tracking (identical to `FileSkipped`) and increments `files_unchanged` on the
   destination. The destination is authoritative for `files_unchanged` (consistent with §7.7).
   No filesystem mutation occurs for a skipped file.

**When the manifest is empty (fallback to baseline behavior):**
- The directory was freshly created (not reused).
- Neither `--overwrite` nor `--ignore-existing` is active.
- The directory's pre-existing entry count exceeds `--overwrite-manifest-max-entries` (default
  5,000,000). This cap bounds memory usage for pathological cases; it is a backstop, not a
  normal limit. When exceeded, the manifest is omitted for that directory and the source
  transfers-and-drains all its files as usual.

**TOCTOU/safety:** The manifest is built fd-relatively on the pinned directory handle, so
names are never re-resolved. A skip performs no filesystem mutation; the destination's existing
`process_single_file` overwrite path still runs for files the source does send. The design's
containment and permission-fidelity guarantees are therefore unchanged.

**Point-in-time observation (not a re-validation at send time):** the skip decision compares
two snapshots captured during the *scan* — the source entry from the Pass-2 directory
enumeration and the destination entry from the Pass-1 manifest build — rather than re-fstatting
at the moment of the decision. This differs from the transfer path, which re-derives the
source metadata from the opened fd (`docs/tocttou.md` Guarantee 2 — the wire header must
describe the bytes actually sent) and re-checks the destination entry at receipt in
`process_single_file`. The consequence is purely about freshness, not safety: a file the
manifest shows as identical is left untouched even if the source or destination entry is
concurrently modified (or the destination entry removed) between the scan and the end of the
copy. This is consistent with rcp's point-in-time, non-atomic copy semantics — concurrent
external modification of the source or destination *during* a copy is never guaranteed to be
reflected (even the transfer path stops re-checking a file once it has handled it; the skip
path's observation point is simply earlier). Crucially, because a skip reads and writes nothing
and emits no header describing un-sent bytes, it cannot violate the containment or
permission-fidelity guarantees — Guarantee 2 governs the data path and does not apply when no
data is sent. If a copy must reflect an actively-changing source or destination, do not rely on
the skip optimization for that run (e.g. quiesce writers, or omit `--overwrite`/`--ignore-existing`).

**Limitation — single root-file copy:** When copying a single file (e.g. `rcp h1:/a/file
h2:/b/file --overwrite`), there is no parent-directory `DirectoryCreated` message to carry a
manifest. This case is not optimized: the source always transfers the file and the destination
drains it.

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
- ✅ Destination directory metadata errors continue without `--fail-early` (sudo test)

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
