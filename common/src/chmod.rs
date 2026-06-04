//! Recursive permission/ownership changes (chmod/chgrp/chown) over a fileset.
//!
//! The public entry point is [`chmod`]; it mirrors [`crate::rm()`] but transforms
//! metadata in place (from a per-type rule) instead of removing entries.
use crate::filter::TimeFilter;
use crate::preserve::Metadata as _;
use crate::progress::Progress;
use crate::safedir::{self, Dir, FileMeta, Handle};
use crate::walk::{EntryKind, LeafPermit, PermitKind};
use crate::walk_driver::{
    DirAction, DirPreResult, EntryCx, ProcessedChildren, WalkVisitor, process_entry,
};
use anyhow::{Context, anyhow};
use std::ffi::OsStr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::instrument;

/// The full 12-bit (`0o7777`) mode of a metadata snapshot, including the
/// setuid/setgid/sticky bits. [`FileMeta`] exposes its mode only through
/// `permissions()`, so this is the canonical way `compute_plan`'s inputs are
/// derived from an fd-pinned [`Handle`].
fn mode_of(meta: &FileMeta) -> u32 {
    meta.permissions().mode() & 0o7777
}

/// Error type for chmod operations. See [`crate::error::OperationError`].
pub type Error = crate::error::OperationError<Summary>;

/// Which user/group id (if any) to apply to each entry type. `None` leaves that
/// type unchanged for this operation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OwnerProgram {
    pub file: Option<u32>,
    pub dir: Option<u32>,
    pub symlink: Option<u32>,
}

impl OwnerProgram {
    #[must_use]
    pub fn for_kind(&self, kind: EntryKind) -> Option<u32> {
        match kind {
            EntryKind::Dir => self.dir,
            EntryKind::Symlink => self.symlink,
            EntryKind::File | EntryKind::Special => self.file,
        }
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.file.is_none() && self.dir.is_none() && self.symlink.is_none()
    }
}

/// A parsed `chmod` mode expression: either a symbolic program (applied relative
/// to the current mode) or an absolute octal value (12-bit).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModeSpec {
    Symbolic(Vec<SymbolicClause>),
    Octal(u32),
}

/// One `[ugoa][+-=][rwxXst]` clause. `who`/`perms` are bitmasks (see `WHO_*` /
/// `PERM_*`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SymbolicClause {
    pub who: u8,
    pub op: ModeOp,
    pub perms: u8,
}

/// The operator in a symbolic mode clause: add, remove, or set permissions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ModeOp {
    Add,
    Remove,
    Set,
}

pub(crate) const WHO_U: u8 = 0b001;
pub(crate) const WHO_G: u8 = 0b010;
pub(crate) const WHO_O: u8 = 0b100;
pub(crate) const WHO_A: u8 = WHO_U | WHO_G | WHO_O;
pub(crate) const PERM_R: u8 = 0b00_0001;
pub(crate) const PERM_W: u8 = 0b00_0010;
pub(crate) const PERM_X: u8 = 0b00_0100;
pub(crate) const PERM_BIGX: u8 = 0b00_1000;
pub(crate) const PERM_S: u8 = 0b01_0000;
pub(crate) const PERM_T: u8 = 0b10_0000;

/// Per-type mode rules. Symlinks are never included (mode bits aren't settable
/// on Linux symlinks).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModeProgram {
    pub file: Option<ModeSpec>,
    pub dir: Option<ModeSpec>,
}

impl ModeProgram {
    #[must_use]
    pub fn for_kind(&self, kind: EntryKind) -> Option<&ModeSpec> {
        match kind {
            EntryKind::Dir => self.dir.as_ref(),
            EntryKind::Symlink => None,
            EntryKind::File | EntryKind::Special => self.file.as_ref(),
        }
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.file.is_none() && self.dir.is_none()
    }
}

/// Configuration for a recursive chmod/chgrp/chown run.
#[derive(Clone, Debug)]
pub struct Settings {
    pub mode: ModeProgram,
    pub owner: OwnerProgram,
    pub group: OwnerProgram,
    pub fail_early: bool,
    /// Apply directory mode/owner changes after their contents (post-order) instead of
    /// before (the default). Needed when recursively removing the owner's own traversal
    /// permission from directories.
    pub defer_dir_changes: bool,
    pub filter: Option<crate::filter::FilterSettings>,
    pub time_filter: Option<TimeFilter>,
    pub dry_run: Option<crate::config::DryRunMode>,
}

#[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Summary {
    pub files_changed: usize,
    pub symlinks_changed: usize,
    pub directories_changed: usize,
    pub files_unchanged: usize,
    pub symlinks_unchanged: usize,
    pub directories_unchanged: usize,
    pub files_skipped: usize,
    pub symlinks_skipped: usize,
    pub directories_skipped: usize,
}

impl std::ops::Add for Summary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self {
            files_changed: self.files_changed + other.files_changed,
            symlinks_changed: self.symlinks_changed + other.symlinks_changed,
            directories_changed: self.directories_changed + other.directories_changed,
            files_unchanged: self.files_unchanged + other.files_unchanged,
            symlinks_unchanged: self.symlinks_unchanged + other.symlinks_unchanged,
            directories_unchanged: self.directories_unchanged + other.directories_unchanged,
            files_skipped: self.files_skipped + other.files_skipped,
            symlinks_skipped: self.symlinks_skipped + other.symlinks_skipped,
            directories_skipped: self.directories_skipped + other.directories_skipped,
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "files changed: {}\n\
            symlinks changed: {}\n\
            directories changed: {}\n\
            files unchanged: {}\n\
            symlinks unchanged: {}\n\
            directories unchanged: {}\n\
            files skipped: {}\n\
            symlinks skipped: {}\n\
            directories skipped: {}\n",
            self.files_changed,
            self.symlinks_changed,
            self.directories_changed,
            self.files_unchanged,
            self.symlinks_unchanged,
            self.directories_unchanged,
            self.files_skipped,
            self.symlinks_skipped,
            self.directories_skipped
        )
    }
}

/// Whether a DSL id token refers to a user or group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdKind {
    User,
    Group,
}

impl IdKind {
    /// The getent database to query for this kind.
    fn getent_database(self) -> &'static str {
        match self {
            IdKind::User => "passwd",
            IdKind::Group => "group",
        }
    }
    /// Human label for error messages.
    fn label(self) -> &'static str {
        match self {
            IdKind::User => "user",
            IdKind::Group => "group",
        }
    }
}

/// Resolve a DSL id token to a numeric id. All-numeric tokens are used directly;
/// otherwise the token is looked up as a user/group name (matching `chown`/`chgrp`):
/// first in-process (reads /etc/passwd & /etc/group; full NSS on dynamic-glibc
/// builds), then via the host `getent` tool (located per `getent`), whose full NSS
/// sees directory-service (LDAP/SSSD/NIS) names that static musl builds cannot.
fn resolve_id(token: &str, kind: IdKind, getent: &GetentResolver) -> anyhow::Result<u32> {
    if let Ok(n) = token.parse::<u32>() {
        return Ok(n);
    }
    let in_process = match kind {
        IdKind::User => {
            nix::unistd::User::from_name(token).map(|user| user.map(|u| u.uid.as_raw()))
        }
        IdKind::Group => {
            nix::unistd::Group::from_name(token).map(|group| group.map(|g| g.gid.as_raw()))
        }
    };
    match in_process {
        Ok(Some(id)) => Ok(id),
        // a miss or an in-process lookup error both fall through to getent: the host
        // tool is the authoritative lookup (full NSS), and on a real miss its
        // "unknown user/group" verdict is the one worth reporting.
        Ok(None) | Err(_) => resolve_via_getent(token, kind, getent),
    }
}

/// `getent` exit status for "key not found" (glibc convention; other getent
/// implementations may report a miss differently — those land in the generic
/// failure arm, which still errors out but with the raw exit status).
const GETENT_NOT_FOUND: i32 = 2;

/// Resolve a name through the host `getent` tool. The host's getent is linked
/// against the host libc with full NSS, so it sees directory-service entries
/// (LDAP/SSSD/NIS) that are invisible to the in-process lookup in static builds
/// (musl has no NSS and reads only /etc/passwd and /etc/group).
///
/// The binary to spawn comes from `getent` (see [`GetentResolver`]): an explicit
/// `--getent-path`, a trusted-directory probe when privileged, or a normal PATH
/// search when unprivileged. PATH is consulted only in that last, unprivileged case.
fn resolve_via_getent(token: &str, kind: IdKind, getent: &GetentResolver) -> anyhow::Result<u32> {
    match getent.program()? {
        Some(path) => resolve_via_getent_cmd(path.as_os_str(), token, kind),
        None => resolve_via_getent_cmd(OsStr::new("getent"), token, kind),
    }
}

/// The [`resolve_via_getent`] body with the getent program injectable for tests.
///
/// `getent_program` is passed to [`std::process::Command::new`] as an `OsStr` — never
/// lossily stringified first — so an exact `--getent-path` is spawned byte-for-byte even
/// if it is not valid UTF-8. The lossy form is used only to render diagnostics.
fn resolve_via_getent_cmd(
    getent_program: &OsStr,
    token: &str,
    kind: IdKind,
) -> anyhow::Result<u32> {
    let database = kind.getent_database();
    let label = kind.label();
    // diagnostics only — the spawn above uses the OsStr verbatim.
    let prog = getent_program.to_string_lossy();
    // `--` terminates getent's option parsing, so a name that looks like an option (e.g.
    // `--service=files`, reachable via `--group=--service=files`) is treated as the lookup
    // key, not an option. Without it, GNU getent would honor the option, print the whole
    // database, and this parser would take the first line's id — silently resolving a bogus
    // name to uid/gid 0 instead of failing.
    let output = std::process::Command::new(getent_program)
        .args(["--", database, token])
        .output()
        .with_context(|| {
            format!(
                "cannot run `{prog} {database} {token}` to look up the {label} name; \
                 use a numeric id instead"
            )
        })?;
    if output.status.code() == Some(GETENT_NOT_FOUND) {
        return Err(anyhow!("unknown {label}: {token}"));
    }
    if !output.status.success() {
        let status = output.status;
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        let detail = if stderr.is_empty() {
            String::new()
        } else {
            format!(": {stderr}")
        };
        return Err(anyhow!(
            "`{prog} {database} {token}` failed with {status}{detail}; \
             use a numeric id instead"
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .next()
        .ok_or_else(|| anyhow!("`{prog} {database} {token}` produced no output"))?;
    parse_getent_id(line).with_context(|| format!("unexpected getent output {line:?}"))
}

/// Directories searched for `getent` when running privileged, in order. PATH is
/// **not** consulted in that case: a privileged `rchm` (e.g. via sudo) must never exec
/// a binary that an unprivileged caller could have planted earlier on PATH. These dirs
/// are root-owned on a sane system; `/run/current-system/sw/bin` covers NixOS, where
/// system tools do not live in `/usr/bin`.
const TRUSTED_GETENT_DIRS: &[&str] = &["/usr/bin", "/bin", "/run/current-system/sw/bin"];

/// Whether this process runs with elevated privilege: effective-root, or a setuid
/// mismatch (real ≠ effective uid). rchm is not installed setuid, but the mismatch
/// check is cheap defense-in-depth. The check keys on uid, not Linux capabilities, so a
/// `CAP_CHOWN` deployment without effective-root is treated as unprivileged (there the
/// PATH is the operator's own, not a third party's).
#[must_use]
pub fn is_privileged() -> bool {
    let euid = nix::unistd::geteuid();
    euid.as_raw() == 0 || nix::unistd::getuid() != euid
}

/// Decides which `getent` binary name resolution spawns, hardened against PATH
/// attacks when privileged. Built once from the CLI via [`GetentResolver::from_cli`].
///
/// The decision is deliberately *not* made at construction time: a numeric-only
/// invocation (`--owner 0`) never needs `getent`, so the trusted-directory probe — and
/// its "getent not found" error — must not fire then. The probe runs only when a name is
/// actually looked up.
#[derive(Clone, Debug)]
pub struct GetentResolver {
    /// An explicit `--getent-path`, validated absolute. Used verbatim, bypassing PATH.
    explicit: Option<PathBuf>,
    /// Whether to harden the unset case (privileged → trusted-dir probe, not PATH).
    privileged: bool,
}

/// The unprivileged, no-override default: a normal PATH search. Used by tests and as a
/// sensible base; production builds the resolver from the CLI via [`GetentResolver::from_cli`].
impl Default for GetentResolver {
    fn default() -> Self {
        Self {
            explicit: None,
            privileged: false,
        }
    }
}

impl GetentResolver {
    /// Build from the parsed CLI. `getent_path` is an explicit `--getent-path` (already
    /// reduced to at most one by the caller); `privileged` is [`is_privileged`]. A
    /// relative `--getent-path` is rejected here (fail-fast) — a relative program would
    /// re-introduce a PATH/cwd lookup, defeating the point.
    pub fn from_cli(getent_path: Option<PathBuf>, privileged: bool) -> anyhow::Result<Self> {
        if let Some(path) = &getent_path
            && !path.is_absolute()
        {
            return Err(anyhow!(
                "--getent-path must be an absolute path, got {path:?}"
            ));
        }
        Ok(Self {
            explicit: getent_path,
            privileged,
        })
    }

    /// The `getent` binary to spawn, resolved on demand. `None` means "search PATH
    /// normally" — returned only in the unprivileged, no-override case.
    fn program(&self) -> anyhow::Result<Option<PathBuf>> {
        self.program_in(TRUSTED_GETENT_DIRS)
    }

    /// [`Self::program`] with the trusted-directory list injected for tests.
    fn program_in(&self, trusted_dirs: &[&str]) -> anyhow::Result<Option<PathBuf>> {
        if let Some(path) = &self.explicit {
            return Ok(Some(path.clone()));
        }
        if !self.privileged {
            // unprivileged: a normal PATH search is fine — there is no privilege boundary
            // to protect, and the caller's PATH (e.g. a nix profile) is what they expect.
            return Ok(None);
        }
        // privileged: never consult PATH — find getent in a trusted, root-owned directory.
        for dir in trusted_dirs {
            let candidate = Path::new(dir).join("getent");
            if candidate.is_file() {
                return Ok(Some(candidate));
            }
        }
        Err(anyhow!(
            "running with elevated privilege and could not find `getent` in any trusted \
             directory ({}); PATH is intentionally ignored when privileged so a name lookup \
             cannot exec an attacker-controlled binary as root — pass an absolute \
             --getent-path, or use numeric ids",
            trusted_dirs.join(", ")
        ))
    }
}

/// Parse the numeric id out of a `getent passwd`/`getent group` line: the third
/// colon-separated field is the uid (passwd) or gid (group).
fn parse_getent_id(line: &str) -> anyhow::Result<u32> {
    let field = line
        .split(':')
        .nth(2)
        .ok_or_else(|| anyhow!("expected at least 3 ':'-separated fields"))?;
    field
        .parse::<u32>()
        .with_context(|| format!("parsing id field {field:?}"))
}

/// Parse a `--group`/`--owner` DSL string. A bare token is the default for all
/// types; `f:`/`d:`/`l:` sections override per type. `getent` locates the `getent`
/// binary used to resolve any non-numeric, NSS-only names (see [`GetentResolver`]).
pub fn parse_owner_dsl(
    s: &str,
    kind: IdKind,
    getent: &GetentResolver,
) -> anyhow::Result<OwnerProgram> {
    let mut prog = OwnerProgram::default();
    let mut bare: Option<u32> = None;
    for clause in s.split_whitespace() {
        if let Some((ty, rest)) = clause.split_once(':') {
            let id = resolve_id(rest, kind, getent)?;
            match ty {
                "f" | "file" => prog.file = Some(id),
                "d" | "dir" | "directory" => prog.dir = Some(id),
                "l" | "link" | "symlink" => prog.symlink = Some(id),
                _ => return Err(anyhow!("unknown type prefix {ty:?} (expected f:/d:/l:)")),
            }
        } else if bare.is_some() {
            return Err(anyhow!(
                "multiple bare values in {s:?}; use f:/d:/l: prefixes to set different types"
            ));
        } else {
            bare = Some(resolve_id(clause, kind, getent)?);
        }
    }
    if let Some(b) = bare {
        prog.file.get_or_insert(b);
        prog.dir.get_or_insert(b);
        prog.symlink.get_or_insert(b);
    }
    Ok(prog)
}

/// Apply a mode spec to a current 12-bit mode, returning the new 12-bit mode.
/// `is_dir` drives the conditional `X` permission.
#[must_use]
pub fn apply_mode(current: u32, spec: &ModeSpec, is_dir: bool) -> u32 {
    match spec {
        ModeSpec::Octal(m) => m & 0o7777,
        ModeSpec::Symbolic(clauses) => {
            let mut mode = current & 0o7777;
            for clause in clauses {
                mode = apply_clause(mode, *clause, is_dir);
            }
            mode
        }
    }
}

fn apply_clause(current: u32, clause: SymbolicClause, is_dir: bool) -> u32 {
    let any_exec = current & 0o111 != 0;
    let exec =
        (clause.perms & PERM_X != 0) || (clause.perms & PERM_BIGX != 0 && (is_dir || any_exec));
    let r = clause.perms & PERM_R != 0;
    let w = clause.perms & PERM_W != 0;
    let s = clause.perms & PERM_S != 0;
    let t = clause.perms & PERM_T != 0;
    let mut value: u32 = 0;
    if clause.who & WHO_U != 0 {
        if r {
            value |= 0o400;
        }
        if w {
            value |= 0o200;
        }
        if exec {
            value |= 0o100;
        }
        if s {
            value |= 0o4000;
        }
    }
    if clause.who & WHO_G != 0 {
        if r {
            value |= 0o040;
        }
        if w {
            value |= 0o020;
        }
        if exec {
            value |= 0o010;
        }
        if s {
            value |= 0o2000;
        }
    }
    if clause.who & WHO_O != 0 {
        if r {
            value |= 0o004;
        }
        if w {
            value |= 0o002;
        }
        if exec {
            value |= 0o001;
        }
    }
    if t && clause.who & WHO_O != 0 {
        // sticky (t) responds only to 'o' (and 'a', which includes 'o') — matches chmod
        value |= 0o1000;
    }
    match clause.op {
        ModeOp::Add => current | value,
        ModeOp::Remove => current & !value,
        ModeOp::Set => {
            let mut clear: u32 = 0;
            if clause.who & WHO_U != 0 {
                clear |= 0o4700;
            }
            if clause.who & WHO_G != 0 {
                clear |= 0o2070;
            }
            if clause.who & WHO_O != 0 {
                clear |= 0o1007;
            }
            (current & !clear) | value
        }
    }
}

/// Parse one mode token: an octal literal (all octal digits) or a comma-chained
/// symbolic expression.
fn parse_mode_token(token: &str) -> anyhow::Result<ModeSpec> {
    if token.is_empty() {
        return Err(anyhow!("empty mode"));
    }
    if token.bytes().all(|b| b.is_ascii_digit()) {
        if token.bytes().any(|b| b > b'7') {
            return Err(anyhow!("invalid octal mode {token:?} (digits must be 0-7)"));
        }
        let value = u32::from_str_radix(token, 8)
            .with_context(|| format!("parsing octal mode {token:?}"))?;
        if value > 0o7777 {
            return Err(anyhow!("octal mode {token:?} out of range (max 0o7777)"));
        }
        return Ok(ModeSpec::Octal(value));
    }
    let clauses = token
        .split(',')
        .map(parse_symbolic_clause)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(ModeSpec::Symbolic(clauses))
}

fn parse_symbolic_clause(clause: &str) -> anyhow::Result<SymbolicClause> {
    let op_pos = clause
        .find(['+', '-', '='])
        .ok_or_else(|| anyhow!("mode clause {clause:?} missing +, - or ="))?;
    let (who_str, rest) = clause.split_at(op_pos);
    let op = match &rest[..1] {
        "+" => ModeOp::Add,
        "-" => ModeOp::Remove,
        "=" => ModeOp::Set,
        _ => unreachable!("find guaranteed one of +-="),
    };
    let perms_str = &rest[1..];
    let mut who = 0u8;
    for ch in who_str.chars() {
        who |= match ch {
            'u' => WHO_U,
            'g' => WHO_G,
            'o' => WHO_O,
            'a' => WHO_A,
            other => {
                return Err(anyhow!(
                    "invalid 'who' {other:?} in {clause:?} (expected u/g/o/a)"
                ));
            }
        };
    }
    if who == 0 {
        who = WHO_A;
    }
    let mut perms = 0u8;
    for ch in perms_str.chars() {
        perms |= match ch {
            'r' => PERM_R,
            'w' => PERM_W,
            'x' => PERM_X,
            'X' => PERM_BIGX,
            's' => PERM_S,
            't' => PERM_T,
            other => return Err(anyhow!("invalid permission {other:?} in {clause:?}")),
        };
    }
    Ok(SymbolicClause { who, op, perms })
}

/// Parse a `--mode` DSL string. A bare token is the default for files+dirs;
/// `f:`/`d:` sections override per type. `l:` is rejected (symlink mode bits
/// are not settable on Linux).
pub fn parse_mode_dsl(s: &str) -> anyhow::Result<ModeProgram> {
    let mut prog = ModeProgram::default();
    let mut bare: Option<ModeSpec> = None;
    for clause in s.split_whitespace() {
        if let Some((ty, rest)) = clause.split_once(':') {
            let spec = parse_mode_token(rest)?;
            match ty {
                "f" | "file" => prog.file = Some(spec),
                "d" | "dir" | "directory" => prog.dir = Some(spec),
                "l" | "link" | "symlink" => {
                    return Err(anyhow!(
                        "symlink mode (l:) is not settable on Linux; remove the l: section"
                    ));
                }
                _ => return Err(anyhow!("unknown type prefix {ty:?} (expected f:/d:)")),
            }
        } else if bare.is_some() {
            return Err(anyhow!(
                "multiple bare mode expressions in {s:?}; chain sub-ops with commas (e.g. g+r,o+w)"
            ));
        } else {
            bare = Some(parse_mode_token(clause)?);
        }
    }
    if let Some(b) = bare {
        prog.file.get_or_insert(b.clone());
        prog.dir.get_or_insert(b);
    }
    Ok(prog)
}

/// The concrete syscalls to perform for one entry. `chown` carries the
/// `(uid, gid)` to pass to `fchownat` (each `None` means "leave unchanged");
/// `chmod` is the target 12-bit mode. An all-`None` plan is a no-op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EntryPlan {
    pub chown: Option<(Option<u32>, Option<u32>)>,
    pub chmod: Option<u32>,
}

impl EntryPlan {
    pub(crate) fn is_noop(&self) -> bool {
        self.chown.is_none() && self.chmod.is_none()
    }
}

/// Compute the plan for one entry from its current mode/uid/gid. Pure: performs
/// no I/O. `cur_mode` is the full `st_mode` (only the low 12 bits are used).
pub(crate) fn compute_plan(
    cur_mode: u32,
    cur_uid: u32,
    cur_gid: u32,
    kind: EntryKind,
    settings: &Settings,
) -> EntryPlan {
    let cur_mode = cur_mode & 0o7777;
    let uid_change = settings.owner.for_kind(kind).filter(|&u| u != cur_uid);
    let gid_change = settings.group.for_kind(kind).filter(|&g| g != cur_gid);
    let need_chown = uid_change.is_some() || gid_change.is_some();
    let chown = need_chown.then_some((uid_change, gid_change));
    let chmod = if kind == EntryKind::Symlink {
        // symlink mode bits are not settable; never chmod a symlink
        None
    } else if let Some(spec) = settings.mode.for_kind(kind) {
        let desired = apply_mode(cur_mode, spec, kind == EntryKind::Dir);
        // chmod if the value changes, or a chown would clear setuid/setgid we keep.
        // 0o6000 = setuid|setgid; fchownat does not clear the sticky bit (0o1000)
        if desired != cur_mode || (need_chown && desired & 0o6000 != 0) {
            Some(desired)
        } else {
            None
        }
    } else if need_chown && cur_mode & 0o6000 != 0 {
        // no mode rule for this type, but chown clears setuid/setgid -> restore.
        // 0o6000 = setuid|setgid; fchownat does not clear the sticky bit (0o1000)
        Some(cur_mode)
    } else {
        None
    };
    EntryPlan { chown, chmod }
}

fn inc_changed(prog: &Progress, kind: EntryKind) -> Summary {
    match kind {
        EntryKind::Dir => {
            prog.directories_changed.inc();
            Summary {
                directories_changed: 1,
                ..Default::default()
            }
        }
        EntryKind::Symlink => {
            prog.symlinks_changed.inc();
            Summary {
                symlinks_changed: 1,
                ..Default::default()
            }
        }
        EntryKind::File | EntryKind::Special => {
            prog.files_changed.inc();
            Summary {
                files_changed: 1,
                ..Default::default()
            }
        }
    }
}

fn inc_unchanged(prog: &Progress, kind: EntryKind) -> Summary {
    match kind {
        EntryKind::Dir => {
            prog.directories_unchanged.inc();
            Summary {
                directories_unchanged: 1,
                ..Default::default()
            }
        }
        EntryKind::Symlink => {
            prog.symlinks_unchanged.inc();
            Summary {
                symlinks_unchanged: 1,
                ..Default::default()
            }
        }
        EntryKind::File | EntryKind::Special => {
            prog.files_unchanged.inc();
            Summary {
                files_unchanged: 1,
                ..Default::default()
            }
        }
    }
}

fn skipped_summary_for(kind: EntryKind) -> Summary {
    match kind {
        EntryKind::Dir => Summary {
            directories_skipped: 1,
            ..Default::default()
        },
        EntryKind::Symlink => Summary {
            symlinks_skipped: 1,
            ..Default::default()
        },
        EntryKind::File | EntryKind::Special => Summary {
            files_skipped: 1,
            ..Default::default()
        },
    }
}

/// Human-readable description of what a plan changes, for dry-run output.
fn describe_change(cur_mode: u32, cur_uid: u32, cur_gid: u32, plan: &EntryPlan) -> String {
    let mut parts = Vec::new();
    if let Some(mode) = plan.chmod {
        if mode == cur_mode & 0o7777 {
            // chmod re-applied only to restore setuid/setgid that chown clears
            parts.push(format!("mode {mode:04o} (re-applied after chown)"));
        } else {
            parts.push(format!("mode {:04o}->{:04o}", cur_mode & 0o7777, mode));
        }
    }
    if let Some((uid, gid)) = plan.chown {
        if let Some(uid) = uid {
            parts.push(format!("owner {cur_uid}->{uid}"));
        }
        if let Some(gid) = gid {
            parts.push(format!("group {cur_gid}->{gid}"));
        }
    }
    parts.join(", ")
}

/// Apply a computed plan to a single entry through the `O_PATH` [`Handle`] we
/// already hold, never re-resolving the entry by path.
///
/// The handle is pinned to the exact inode (opened `O_NOFOLLOW`), so both syscalls
/// act on that inode rather than on a name: there is no TOCTOU window and no
/// `recheck` is needed (unlike copy's name-based overwrite). A concurrent
/// rename/symlink swap of the directory entry cannot redirect either operation to a
/// different target.
///
/// Syscall choice, following the documented chown → chmod ordering:
/// * **chown** — [`safedir::fchown_handle`] (inode-exact `fchownat` with
///   `AT_EMPTY_PATH | AT_SYMLINK_NOFOLLOW`). Applies to files, dirs, AND symlinks.
/// * **chmod** — [`safedir::chmod_via_proc_fd`] (chmod of the inode via its
///   `/proc/self/fd` magic symlink). Used for both files and directories: `fchmod`
///   is `EBADF` on the `O_PATH` handle, and the `/proc` path is inode-exact, works
///   on all kernels, and crucially works even on a `0000`-mode directory we own
///   (so a pre-order `d:u+rwx` can recover an unreadable directory before we open
///   it to recurse). Symlinks are never chmod'd — [`compute_plan`] guarantees
///   `plan.chmod` is `None` for a symlink.
async fn apply_plan(handle: &Handle, plan: &EntryPlan) -> anyhow::Result<()> {
    if let Some((uid, gid)) = plan.chown {
        safedir::fchown_handle(handle, congestion::Side::Destination, uid, gid)
            .await
            .with_context(|| format!("failed to chown via fd (uid={uid:?}, gid={gid:?})"))?;
    }
    if let Some(mode) = plan.chmod {
        safedir::chmod_via_proc_fd(handle, congestion::Side::Destination, mode)
            .await
            .with_context(|| format!("failed to chmod via fd to {mode:04o}"))?;
    }
    Ok(())
}

/// Apply the change to a single entry (a leaf, or a directory after its children
/// were processed). Handles the time filter, dry-run, no-op skip, and counters.
///
/// `handle` is the entry's fd-pinned `O_PATH` [`Handle`]; its [`Handle::meta`]
/// snapshot feeds [`compute_plan`] (replacing the old path-based `symlink_metadata`)
/// and is the target of the inode-exact chown/chmod. `path` is the reconstructed
/// display path used purely for dry-run output and diagnostics.
async fn apply_entry_change(
    prog: &'static Progress,
    path: &std::path::Path,
    handle: &Handle,
    kind: EntryKind,
    settings: &Settings,
) -> Result<Summary, Error> {
    if let Some(ref time_filter) = settings.time_filter {
        // the time filter needs full std metadata (btime for --created-before), which
        // the fd snapshot does not carry; read it inode-exact through the pinned handle.
        let metadata =
            match safedir::stat_meta_via_proc_fd(handle, congestion::Side::Destination).await {
                Ok(md) => md,
                Err(err) => {
                    let err = anyhow::Error::new(err).context(format!(
                        "failed reading metadata for time filter on {path:?}"
                    ));
                    if settings.fail_early {
                        return Err(Error::new(err, Default::default()));
                    }
                    tracing::warn!("time filter failed for {:?}, skipping: {:#}", path, &err);
                    kind.inc_skipped(prog);
                    return Ok(skipped_summary_for(kind));
                }
            };
        match time_filter.matches(&metadata) {
            Ok(result) => {
                if let Some(reason) = result.as_skip_reason() {
                    if let Some(mode) = settings.dry_run {
                        crate::dry_run::report_time_skip(path, reason, mode, kind.label());
                    }
                    kind.inc_skipped(prog);
                    return Ok(skipped_summary_for(kind));
                }
            }
            Err(err) => {
                let err = err.context(format!("failed evaluating time filter on {path:?}"));
                if settings.fail_early {
                    return Err(Error::new(err, Default::default()));
                }
                tracing::warn!("time filter failed for {:?}, skipping: {:#}", path, &err);
                kind.inc_skipped(prog);
                return Ok(skipped_summary_for(kind));
            }
        }
    }
    let meta = handle.meta();
    let cur_mode = mode_of(meta);
    let plan = compute_plan(cur_mode, meta.uid(), meta.gid(), kind, settings);
    if plan.is_noop() {
        if let Some(crate::config::DryRunMode::All) = settings.dry_run {
            println!("unchanged {} {:?}", kind.label(), path);
        }
        return Ok(inc_unchanged(prog, kind));
    }
    if settings.dry_run.is_some() {
        let desc = describe_change(cur_mode, meta.uid(), meta.gid(), &plan);
        println!("would modify {} {:?}: {}", kind.label(), path, desc);
        return Ok(inc_changed(prog, kind));
    }
    apply_plan(handle, &plan)
        .await
        .map_err(|err| Error::new(err, Default::default()))?;
    Ok(inc_changed(prog, kind))
}

/// Public entry point. Applies metadata changes to `path` and, recursively, its
/// contents. Mirrors [`crate::rm::rm`] for the root-filter check.
///
/// The walk is fd-based (see [`crate::safedir`]): the root operand is opened
/// relative to its parent directory and every entry is classified and mutated
/// through file-descriptor-relative syscalls. A privileged `rchm` therefore cannot
/// be redirected by a concurrent symlink swap into chmod/chown'ing a target outside
/// the intended tree — the `O_NOFOLLOW` opens and the inode-pinned `O_PATH` handles
/// catch the swap and fail closed.
#[instrument(skip(prog_track, settings))]
pub async fn chmod(
    prog_track: &'static Progress,
    path: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    // decompose the operand into (parent dir, final component) so the root entry is opened and
    // classified relative to a directory fd — the same fd-relative shape every nested entry takes.
    // rchm mutates "the" tree in place, so the parent is opened on the Destination side. `.`/`..`
    // operands (e.g. `rchm -R … .`) are canonicalized so they still name a directory; `/` is
    // rejected.
    let operand = crate::walk::split_root_operand(path)
        .await
        .map_err(|err| Error::new(err, Default::default()))?;
    let parent_path = operand.parent.as_path();
    let name = operand.name.as_os_str();
    let path = operand.display.as_path();
    // the operand's TRUSTED parent prefix is resolved following symlinks normally (the prefix is
    // trusted up to and including the operand's container — only entries strictly below the named
    // root are O_NOFOLLOW-hardened). a symlinked parent (e.g. `rchm symlinkdir/foo`) is followed; the
    // operand itself is still classified via `child(name)` with O_NOFOLLOW (a symlink root is
    // operated on as the link itself).
    let parent = Dir::open_parent_dir(parent_path, congestion::Side::Destination)
        .await
        .with_context(|| format!("cannot open parent directory {parent_path:?}"))
        .map_err(|err| Error::new(err, Default::default()))?;
    // cross from the trusted parent prefix into the hardened tree (O_NOFOLLOW below here).
    let parent = Arc::new(parent.into_tree());
    if let Some(ref filter) = settings.filter {
        // classify the root via its parent fd purely to evaluate the root filter; the driver
        // re-classifies the root authoritatively in `process_entry`, so this handle is just a probe.
        let root_handle = parent
            .child(name)
            .await
            .with_context(|| format!("failed reading metadata from {path:?}"))
            .map_err(|err| Error::new(err, Default::default()))?;
        let name_path = std::path::Path::new(name);
        match filter.should_include_root_item(name_path, root_handle.kind() == EntryKind::Dir) {
            crate::filter::FilterResult::Included => {}
            result => {
                let kind = root_handle.kind();
                if let Some(mode) = settings.dry_run {
                    crate::dry_run::report_skip(path, &result, mode, kind.label_long());
                }
                kind.inc_skipped(prog_track);
                return Ok(skipped_summary_for(kind));
            }
        }
    }
    run_chmod_root(prog_track, &parent, name, path, settings).await
}

/// Build the [`ChmodVisitor`] and process the root entry through the generic
/// [`crate::walk_driver`] driver. The root is processed exactly like a nested child: classified
/// authoritatively, then dispatched to `visit_leaf` or `dir_pre`/recurse/`dir_post`.
async fn run_chmod_root(
    prog_track: &'static Progress,
    parent: &Arc<Dir>,
    name: &OsStr,
    root: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    let visitor = Arc::new(ChmodVisitor {
        prog_track,
        settings: settings.clone(),
    });
    // the root entry's owned context: rel_path/filter_path empty (the root), real_path = the root
    // operand. chmod has no filter base (no delegated subtree), so `filter_path == rel_path`.
    let root_cx = EntryCx {
        parent: Arc::clone(parent),
        name: name.to_owned(),
        rel_path: PathBuf::new(),
        filter_path: PathBuf::new(),
        real_path: root.to_path_buf(),
        dry_run: settings.dry_run.is_some(),
        prog_track,
    };
    process_entry(visitor, root_cx, (), None).await // chmod has no second tree → root context `()`
}

/// The chmod walk's [`WalkVisitor`]. The driver owns enumeration, the leaf-permit lifecycle,
/// spawning, the single drop-before-recurse site, and the error fold; this visitor supplies
/// chmod's per-entry bodies (`apply_entry_change` for leaves, `apply_dir_self` for the directory's
/// own pre-/post-order change, `open_dir` for descent). chmod has no second tree, so
/// [`Self::DirContext`] is `()`.
struct ChmodVisitor {
    prog_track: &'static Progress,
    settings: Settings,
}

/// State threaded from [`WalkVisitor::dir_pre`] to [`WalkVisitor::dir_post`] (same task) — what
/// `dir_post` needs to apply the directory's own change.
struct ChmodDirState {
    /// The directory's owned `O_PATH` handle; the post-order chmod goes through its `/proc/self/fd`
    /// magic symlink, inode-exact (works even on a `0000`-mode directory).
    handle: Handle,
    /// Only traversed to find include-matches (not directly matched), so its own mode/owner is left
    /// unchanged (mirrors rm.rs).
    traversed_only: bool,
    /// The directory's own pre-order contribution, folded into the final summary by `dir_post`.
    base: Summary,
    /// A pre-order change error captured in keep-going mode (the descent still proceeded, so
    /// `dir_post` surfaces it). `None` when the pre-order change succeeded or was deferred —
    /// mutually exclusive with the post-order change `dir_post` applies under `defer_dir_changes`.
    pre_order_error: Option<anyhow::Error>,
}

impl WalkVisitor for ChmodVisitor {
    type Summary = Summary;
    type DirContext = ();
    type DirState = ChmodDirState;

    fn root_dir_context(&self) {}

    fn permit_kind(&self) -> PermitKind {
        // metadata-only walk: no open fd held across leaf work, so gate on the pending-meta pool.
        PermitKind::PendingMeta
    }

    fn want_permit(&self, hint: Option<EntryKind>) -> bool {
        // known non-directory hint only (a hinted dir recurses; DT_UNKNOWN might be a dir) —
        // matches the old spawn loop's `known_leaf` pre-acquire policy.
        hint.is_some_and(|k| k != EntryKind::Dir)
    }

    fn fail_early(&self) -> bool {
        self.settings.fail_early
    }

    fn filter(&self) -> Option<&crate::filter::FilterSettings> {
        self.settings.filter.as_ref()
    }

    fn on_skip(
        &self,
        cx: &EntryCx,
        kind: EntryKind,
        skip_result: &crate::filter::FilterResult,
    ) -> Summary {
        // mirror the old spawn loop's inline filter-skip: the dry-run "skip ..." line plus the
        // matching `*_skipped` counter. the driver already did the shared progress increment.
        if let Some(mode) = self.settings.dry_run {
            crate::dry_run::report_skip(&cx.real_path, skip_result, mode, kind.label());
        }
        skipped_summary_for(kind)
    }

    async fn visit_leaf(
        &self,
        cx: &EntryCx,
        _parent_ctx: &(),
        handle: Handle,
        kind: EntryKind,
        permit: Option<LeafPermit>,
    ) -> Result<Summary, Error> {
        // the leaf change (time-filter + compute_plan + apply_plan), exactly as the old leaf branch.
        // the permit is held across this non-recursive work and dropped on return (the driver drops
        // it for directories, never here).
        let _permit = permit;
        apply_entry_change(
            self.prog_track,
            &cx.real_path,
            &handle,
            kind,
            &self.settings,
        )
        .await
    }

    async fn dir_pre(&self, cx: &EntryCx, _parent_ctx: &(), handle: &Handle) -> DirPreResult<Self> {
        let path = &cx.real_path;
        // a "traversed-only" dir was entered only because it COULD contain include-matches; it does
        // not directly match an include pattern, so its own mode/owner is left unchanged (mirrors
        // rm.rs) — only directly-selected entries are modified.
        let traversed_only = self.settings.filter.as_ref().is_some_and(|f| {
            f.has_includes() && !f.directly_matches_include(&cx.filter_path, true)
        });
        let mut base = Summary::default();
        let mut pre_order_error: Option<anyhow::Error> = None;
        // pre-order (default, like `chmod -R`): change the dir BEFORE descending, via the O_PATH
        // handle's /proc path (works even on a 0000 dir) — so `--mode d:u+rwx` recovers an
        // unreadable dir before the open reads it, and a later child failure can't prevent it. a
        // restrictive change (e.g. `d:a-rwx`) makes the open fail and is reported (like `chmod -R`).
        if !self.settings.defer_dir_changes {
            match apply_dir_self(
                self.prog_track,
                path,
                handle,
                traversed_only,
                &self.settings,
            )
            .await
            {
                Ok(dir_summary) => base = base + dir_summary,
                // fail-early: report now, never open or descend. keep-going: stash the error for
                // `dir_post` to surface after the descent, and still open and recurse (old flow).
                Err(error) if self.settings.fail_early => {
                    return Err(Error::new(error.source, base + error.summary));
                }
                Err(error) => {
                    tracing::error!("chmod: {:?} failed with: {:#}", path, &error);
                    base = base + error.summary;
                    pre_order_error = Some(error.source);
                }
            }
        }
        // dup the dir's O_PATH handle (a pure fd dup — no extra openat/fstatat) so `dir_post` can
        // apply the deferred/post-order change inode-exact: the driver lends `&handle` only for
        // `dir_pre`, so the post-order step needs its own owned handle to the same inode.
        let dir_handle = handle
            .try_clone()
            .with_context(|| format!("cannot duplicate directory handle for {path:?}"))
            .map_err(|err| Error::new(err, base))?;
        // open the directory's real fd for the driver to enumerate. in post-order the dir still has
        // its original mode, so the open succeeds and the held fd survives a later search-bit strip.
        // an open failure (restrictive pre-order change, or a pre-existing unreadable dir) is reported.
        match cx.parent.open_dir(&cx.name).await {
            Ok(dir) => Ok(DirAction::Descend {
                dir: Arc::new(dir),
                child_ctx: (),
                state: ChmodDirState {
                    handle: dir_handle,
                    traversed_only,
                    base,
                    pre_order_error,
                },
            }),
            Err(error) => {
                let error = anyhow::Error::new(error)
                    .context(format!("cannot open directory {path:?} for reading"));
                if self.settings.fail_early {
                    return Err(Error::new(error, base));
                }
                // keep-going: no children to walk. fold the pre-order error (if any) + the open
                // error, apply the deferred change (if any), and surface the combined error — the
                // net result of the old open-failure path (empty join loop, then deferred change).
                let errors = crate::error_collector::ErrorCollector::default();
                if let Some(pre_order_error) = pre_order_error {
                    errors.push(pre_order_error);
                }
                tracing::error!("chmod: {:#}", &error);
                errors.push(error);
                if self.settings.defer_dir_changes {
                    match apply_dir_self(
                        self.prog_track,
                        path,
                        handle,
                        traversed_only,
                        &self.settings,
                    )
                    .await
                    {
                        Ok(dir_summary) => base = base + dir_summary,
                        Err(error) => {
                            tracing::error!("chmod: {:?} failed with: {:#}", path, &error);
                            base = base + error.summary;
                            errors.push(error.source);
                        }
                    }
                }
                // `into_error()` is `Some` here: at least the open error was pushed.
                Err(Error::new(errors.into_error().unwrap(), base))
            }
        }
    }

    async fn dir_post(
        &self,
        cx: &EntryCx,
        state: ChmodDirState,
        _processed: &ProcessedChildren,
        child_result: Result<Summary, Error>,
    ) -> Result<Summary, Error> {
        let ChmodDirState {
            handle,
            traversed_only,
            base,
            pre_order_error,
        } = state;
        let path = &cx.real_path;
        // seed with the dir's own pre-order contribution, then fold the children (the driver passes
        // `Err` here only in keep-going mode — fail-early aborts before post-order).
        let (child_summary, child_error) = match child_result {
            Ok(summary) => (summary, None),
            Err(err) => (err.summary, Some(err.source)),
        };
        let mut summary = base + child_summary;
        // collect the deferred pre-order error and the child error (keep-going only).
        let errors = crate::error_collector::ErrorCollector::default();
        if let Some(pre_order_error) = pre_order_error {
            errors.push(pre_order_error);
        }
        if let Some(child_error) = child_error {
            errors.push(child_error);
        }
        // post-order (`--defer-dir-changes`): change the dir AFTER its contents (needed to remove
        // the owner's own traversal permission). the contents were read through a fd opened before
        // this change, so stripping the search bit here can't lock us out; the change is inode-exact
        // through the O_PATH handle. (`pre_order_error` is always `None` here, so the two are
        // mutually exclusive.)
        if self.settings.defer_dir_changes {
            match apply_dir_self(
                self.prog_track,
                path,
                &handle,
                traversed_only,
                &self.settings,
            )
            .await
            {
                Ok(dir_summary) => summary = summary + dir_summary,
                Err(error) => {
                    if self.settings.fail_early {
                        return Err(Error::new(error.source, summary + error.summary));
                    }
                    tracing::error!("chmod: {:?} failed with: {:#}", path, &error);
                    summary = summary + error.summary;
                    errors.push(error.source);
                }
            }
        }
        if let Some(error) = errors.into_error() {
            return Err(Error::new(error, summary));
        }
        Ok(summary)
    }
}

/// Apply the directory's own change, or skip it when the directory was only traversed to
/// find include-matches. Factored out so the walk can run it before (pre-order, default)
/// or after (post-order, `--defer-dir-changes`) descending into the contents.
///
/// `handle` is the directory's `O_PATH` handle; the chmod goes through its `/proc/self/fd`
/// magic symlink, so the change applies inode-exact and works even on a `0000`-mode
/// directory (a pre-order `d:u+rwx` recovers traversability before we open the dir to read).
async fn apply_dir_self(
    prog_track: &'static Progress,
    path: &std::path::Path,
    handle: &Handle,
    traversed_only: bool,
    settings: &Settings,
) -> Result<Summary, Error> {
    if traversed_only {
        if let Some(crate::config::DryRunMode::All) = settings.dry_run {
            println!("skip dir {path:?} (only traversed for include matches)");
        }
        prog_track.directories_skipped.inc();
        return Ok(skipped_summary_for(EntryKind::Dir));
    }
    apply_entry_change(prog_track, path, handle, EntryKind::Dir, settings).await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn mode_token_octal() {
        assert_eq!(parse_mode_token("2775").unwrap(), ModeSpec::Octal(0o2775));
        assert_eq!(parse_mode_token("0644").unwrap(), ModeSpec::Octal(0o644));
    }
    #[test]
    fn mode_token_octal_out_of_range_errors() {
        assert!(parse_mode_token("9999").is_err()); // 9 is not octal
        assert!(parse_mode_token("77777").is_err()); // > 12 bits
    }
    #[test]
    fn mode_token_symbolic_simple() {
        let spec = parse_mode_token("g+w").unwrap();
        assert_eq!(
            spec,
            ModeSpec::Symbolic(vec![SymbolicClause {
                who: WHO_G,
                op: ModeOp::Add,
                perms: PERM_W
            }])
        );
    }
    #[test]
    fn mode_token_symbolic_omitted_who_means_all() {
        let spec = parse_mode_token("+x").unwrap();
        assert_eq!(
            spec,
            ModeSpec::Symbolic(vec![SymbolicClause {
                who: WHO_A,
                op: ModeOp::Add,
                perms: PERM_X
            }])
        );
    }
    #[test]
    fn mode_token_symbolic_comma_chained() {
        let spec = parse_mode_token("u+rw,g-w").unwrap();
        let ModeSpec::Symbolic(clauses) = spec else {
            panic!("expected symbolic")
        };
        assert_eq!(clauses.len(), 2);
        assert_eq!(
            clauses[0],
            SymbolicClause {
                who: WHO_U,
                op: ModeOp::Add,
                perms: PERM_R | PERM_W
            }
        );
        assert_eq!(
            clauses[1],
            SymbolicClause {
                who: WHO_G,
                op: ModeOp::Remove,
                perms: PERM_W
            }
        );
    }
    #[test]
    fn mode_token_symbolic_bigx_and_specials() {
        let spec = parse_mode_token("g+rwXs").unwrap();
        assert_eq!(
            spec,
            ModeSpec::Symbolic(vec![SymbolicClause {
                who: WHO_G,
                op: ModeOp::Add,
                perms: PERM_R | PERM_W | PERM_BIGX | PERM_S,
            }])
        );
    }
    #[test]
    fn mode_token_rejects_garbage() {
        assert!(parse_mode_token("q+z").is_err());
        assert!(parse_mode_token("g!w").is_err());
        assert!(parse_mode_token("").is_err());
    }
    #[test]
    fn summary_add_combines_fields() {
        let a = Summary {
            files_changed: 1,
            directories_changed: 2,
            files_unchanged: 3,
            ..Default::default()
        };
        let b = Summary {
            files_changed: 10,
            symlinks_skipped: 4,
            ..Default::default()
        };
        let sum = a + b;
        assert_eq!(sum.files_changed, 11);
        assert_eq!(sum.directories_changed, 2);
        assert_eq!(sum.files_unchanged, 3);
        assert_eq!(sum.symlinks_skipped, 4);
    }
    #[test]
    fn owner_dsl_bare_applies_to_all_types() {
        let prog = parse_owner_dsl("0", IdKind::User, &GetentResolver::default()).unwrap();
        assert_eq!(prog.file, Some(0));
        assert_eq!(prog.dir, Some(0));
        assert_eq!(prog.symlink, Some(0));
    }
    #[test]
    fn owner_dsl_per_type_overrides() {
        let prog = parse_owner_dsl("f:1 d:2", IdKind::User, &GetentResolver::default()).unwrap();
        assert_eq!(prog.file, Some(1));
        assert_eq!(prog.dir, Some(2));
        assert_eq!(prog.symlink, None);
    }
    #[test]
    fn owner_dsl_bare_plus_override() {
        let prog = parse_owner_dsl("5 d:2", IdKind::Group, &GetentResolver::default()).unwrap();
        assert_eq!(prog.file, Some(5));
        assert_eq!(prog.dir, Some(2));
        assert_eq!(prog.symlink, Some(5));
    }
    #[test]
    fn owner_dsl_explicit_before_bare_is_order_independent() {
        let prog = parse_owner_dsl("f:1 5", IdKind::User, &GetentResolver::default()).unwrap();
        assert_eq!(prog.file, Some(1));
        assert_eq!(prog.dir, Some(5));
        assert_eq!(prog.symlink, Some(5));
    }
    #[test]
    fn owner_dsl_rejects_multiple_bare() {
        assert!(parse_owner_dsl("1 2", IdKind::User, &GetentResolver::default()).is_err());
    }
    #[test]
    fn owner_dsl_rejects_unknown_id() {
        assert!(
            parse_owner_dsl(
                "definitely-no-such-group-xyz",
                IdKind::Group,
                &GetentResolver::default()
            )
            .is_err()
        );
    }
    #[test]
    fn owner_dsl_resolves_root_name() {
        // "root" is in /etc/passwd everywhere — covers the in-process from_name path
        let prog = parse_owner_dsl("root", IdKind::User, &GetentResolver::default()).unwrap();
        assert_eq!(prog.file, Some(0));
        assert_eq!(prog.dir, Some(0));
        assert_eq!(prog.symlink, Some(0));
    }
    #[test]
    fn getent_id_parses_passwd_and_group_lines() {
        // passwd: name:passwd:uid:gid:gecos:home:shell — field 3 is the uid
        assert_eq!(
            parse_getent_id("alice:x:1234:100:Alice:/home/alice:/bin/sh").unwrap(),
            1234
        );
        // group: name:passwd:gid:members — field 3 is the gid
        assert_eq!(parse_getent_id("data:*:5678:alice,bob").unwrap(), 5678);
    }
    #[test]
    fn getent_id_rejects_malformed_lines() {
        assert!(parse_getent_id("").is_err());
        assert!(parse_getent_id("alice").is_err());
        assert!(parse_getent_id("alice:x").is_err());
        assert!(parse_getent_id("alice:x:notanumber:100").is_err());
    }
    /// Write an executable stub script into `dir` and return its path.
    fn write_stub_getent(dir: &std::path::Path, name: &str, body: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }
    #[test]
    fn getent_stub_resolves_directory_service_names() {
        let tmp = tempfile::tempdir().unwrap();
        // simulates an SSSD/LDAP-served entry: visible to getent, absent from /etc files. the
        // guard asserts the args are `-- <database> <token>` — i.e. the `--` option terminator
        // is present and the database lands in the right slot (a wrong database would exit 99).
        let user_stub = write_stub_getent(
            tmp.path(),
            "getent-user",
            "[ \"$1\" = -- ] && [ \"$2\" = passwd ] || exit 99\necho 'ldapuser:*:4242:4242:LDAP User:/home/ldapuser:/bin/sh'",
        );
        let group_stub = write_stub_getent(
            tmp.path(),
            "getent-group",
            "[ \"$1\" = -- ] && [ \"$2\" = group ] || exit 99\necho 'ldapgroup:*:4343:ldapuser'",
        );
        assert_eq!(
            resolve_via_getent_cmd(user_stub.as_os_str(), "ldapuser", IdKind::User).unwrap(),
            4242
        );
        assert_eq!(
            resolve_via_getent_cmd(group_stub.as_os_str(), "ldapgroup", IdKind::Group).unwrap(),
            4343
        );
    }
    #[test]
    fn getent_stub_not_found_is_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        let stub = write_stub_getent(tmp.path(), "getent-miss", "exit 2");
        let err = resolve_via_getent_cmd(stub.as_os_str(), "nosuch", IdKind::Group).unwrap_err();
        assert!(
            format!("{err:#}").contains("unknown group: nosuch"),
            "got: {err:#}"
        );
    }
    #[test]
    fn getent_missing_program_suggests_numeric_id() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("no-such-getent");
        let err = resolve_via_getent_cmd(missing.as_os_str(), "alice", IdKind::User).unwrap_err();
        assert!(
            format!("{err:#}").contains("use a numeric id instead"),
            "got: {err:#}"
        );
    }
    #[test]
    fn getent_stub_garbled_output_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let stub = write_stub_getent(tmp.path(), "getent-garbled", "echo 'not a passwd line'");
        let err = resolve_via_getent_cmd(stub.as_os_str(), "alice", IdKind::User).unwrap_err();
        assert!(
            format!("{err:#}").contains("unexpected getent output"),
            "got: {err:#}"
        );
    }
    #[test]
    fn getent_stub_exit0_no_output_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let stub = write_stub_getent(tmp.path(), "getent-empty", "exit 0");
        let err = resolve_via_getent_cmd(stub.as_os_str(), "alice", IdKind::User).unwrap_err();
        assert!(
            format!("{err:#}").contains("produced no output"),
            "got: {err:#}"
        );
    }
    #[test]
    fn getent_real_resolves_root() {
        // root exists in /etc/passwd and /etc/group on every supported host; this
        // exercises the real `getent` end-to-end (spawn + parse).
        assert_eq!(
            resolve_via_getent("root", IdKind::User, &GetentResolver::default()).unwrap(),
            0
        );
        assert_eq!(
            resolve_via_getent("root", IdKind::Group, &GetentResolver::default()).unwrap(),
            0
        );
    }
    #[test]
    fn getent_real_option_like_name_fails_closed_no_injection() {
        // a name starting with `-` must NOT be parsed as a getent option. Without the `--`
        // terminator, `getent group --service=files` exits 0 and prints the whole database, so
        // this parser would take the first line and silently resolve to gid 0. With `--` the
        // name is a lookup key, found in no DB, so resolution fails closed. Reachable in the CLI
        // via `rchm --group=--service=files` — a bogus name must never map to root.
        let err = resolve_via_getent("--service=files", IdKind::Group, &GetentResolver::default())
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unknown group") || msg.contains("produced no output"),
            "option-like name must fail closed (not resolve to an id), got: {msg}"
        );
    }
    #[test]
    fn getent_source_rejects_relative_explicit_path() {
        // a relative --getent-path would re-introduce a PATH/cwd lookup — rejected up front.
        let err = GetentResolver::from_cli(Some(PathBuf::from("getent")), false).unwrap_err();
        assert!(
            format!("{err:#}").contains("must be an absolute path"),
            "got: {err:#}"
        );
    }
    #[test]
    fn getent_source_explicit_absolute_used_even_when_privileged() {
        // an explicit absolute path is honored verbatim and bypasses the trusted-dir probe.
        let resolver =
            GetentResolver::from_cli(Some(PathBuf::from("/opt/nss/getent")), true).unwrap();
        assert_eq!(
            resolver.program_in(&["/usr/bin"]).unwrap(),
            Some(PathBuf::from("/opt/nss/getent"))
        );
    }
    #[test]
    fn getent_source_unprivileged_searches_path() {
        // unprivileged + no override → None means "search PATH normally".
        let resolver = GetentResolver::from_cli(None, false).unwrap();
        assert_eq!(resolver.program_in(&["/nonexistent"]).unwrap(), None);
    }
    #[test]
    fn getent_source_privileged_probes_trusted_dirs_not_path() {
        // privileged + no override → use getent found in a trusted dir; PATH is never consulted.
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("getent");
        std::fs::write(&bin, "#!/bin/sh\n").unwrap();
        let resolver = GetentResolver::from_cli(None, true).unwrap();
        let dirs = [tmp.path().to_str().unwrap()];
        assert_eq!(resolver.program_in(&dirs).unwrap(), Some(bin));
    }
    #[test]
    fn getent_source_privileged_missing_getent_errors_not_path_fallback() {
        // privileged + no override + getent absent from every trusted dir → hard error,
        // NOT a silent PATH fallback (the core anti-PATH-attack guarantee).
        let tmp = tempfile::tempdir().unwrap(); // empty: no getent inside
        let resolver = GetentResolver::from_cli(None, true).unwrap();
        let dir = tmp.path().to_str().unwrap();
        let err = resolver.program_in(&[dir]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("PATH is intentionally ignored"),
            "should explain PATH is not consulted, got: {msg}"
        );
        assert!(
            msg.contains(dir),
            "should name the searched dir, got: {msg}"
        );
    }
    fn sym(s: &str) -> ModeSpec {
        parse_mode_token(s).unwrap()
    }
    #[test]
    fn apply_mode_group_add_remove() {
        assert_eq!(apply_mode(0o644, &sym("g+w"), false), 0o664);
        assert_eq!(apply_mode(0o664, &sym("g-w"), false), 0o644);
    }
    #[test]
    fn apply_mode_set_clears_other_bits() {
        // o= clears all 'other' bits (incl sticky), leaves user/group
        assert_eq!(apply_mode(0o755, &sym("o="), false), 0o750);
        // chained absolute-ish set from zero
        assert_eq!(apply_mode(0o000, &sym("u=rwx,go=rx"), false), 0o755);
        // o= on a sticky file clears the sticky bit too
        assert_eq!(apply_mode(0o1755, &sym("o="), false), 0o0750);
    }
    #[test]
    fn apply_mode_conditional_bigx() {
        // file without execute: X does nothing
        assert_eq!(apply_mode(0o644, &sym("a+X"), false), 0o644);
        // file with a user-execute bit: X applies to all
        assert_eq!(apply_mode(0o744, &sym("a+X"), false), 0o755);
        // directory: X always applies
        assert_eq!(apply_mode(0o644, &sym("a+X"), true), 0o755);
    }
    #[test]
    fn apply_mode_setgid_and_sticky() {
        assert_eq!(apply_mode(0o750, &sym("g+rwxs"), true), 0o2770);
        assert_eq!(apply_mode(0o755, &sym("+t"), true), 0o1755);
        assert_eq!(apply_mode(0o755, &sym("u+s"), false), 0o4755);
    }
    #[test]
    fn apply_mode_sticky_only_responds_to_other() {
        // u+t / g+t are no-ops; only o/a/bare set sticky (verified against real chmod)
        assert_eq!(apply_mode(0o755, &sym("u+t"), false), 0o755);
        assert_eq!(apply_mode(0o755, &sym("g+t"), false), 0o755);
        assert_eq!(apply_mode(0o755, &sym("ug+t"), false), 0o755);
        assert_eq!(apply_mode(0o755, &sym("o+t"), false), 0o1755);
        assert_eq!(apply_mode(0o755, &sym("+t"), false), 0o1755);
        assert_eq!(apply_mode(0o1755, &sym("u-t"), false), 0o1755);
        assert_eq!(apply_mode(0o1755, &sym("o-t"), false), 0o755);
    }
    #[test]
    fn apply_mode_octal_is_absolute() {
        assert_eq!(apply_mode(0o4755, &sym("644"), false), 0o644);
        assert_eq!(apply_mode(0o000, &sym("2775"), true), 0o2775);
    }
    #[test]
    fn mode_dsl_bare_applies_to_file_and_dir_not_symlink() {
        let prog = parse_mode_dsl("g+rwX").unwrap();
        assert!(prog.file.is_some());
        assert!(prog.dir.is_some());
        // symlinks are not part of ModeProgram at all
        assert!(prog.for_kind(EntryKind::Symlink).is_none());
    }
    #[test]
    fn mode_dsl_per_type() {
        let prog = parse_mode_dsl("f:g+rw d:g+rwxs").unwrap();
        assert_eq!(prog.file, Some(sym("g+rw")));
        assert_eq!(prog.dir, Some(sym("g+rwxs")));
    }
    #[test]
    fn mode_dsl_bare_plus_override() {
        let prog = parse_mode_dsl("g+r d:g+rwx").unwrap();
        assert_eq!(prog.file, Some(sym("g+r")));
        assert_eq!(prog.dir, Some(sym("g+rwx")));
    }
    #[test]
    fn mode_dsl_rejects_symlink_section() {
        assert!(parse_mode_dsl("l:g+w").is_err());
    }
    #[test]
    fn mode_dsl_rejects_multiple_bare() {
        assert!(parse_mode_dsl("g+r o+w").is_err());
    }
    #[test]
    fn mode_dsl_rejects_unknown_prefix() {
        assert!(parse_mode_dsl("z:644").is_err());
    }
    #[test]
    fn mode_dsl_single_type_leaves_other_none() {
        let prog_f = parse_mode_dsl("f:644").unwrap();
        assert!(prog_f.file.is_some());
        assert!(prog_f.dir.is_none());
        let prog_d = parse_mode_dsl("d:755").unwrap();
        assert!(prog_d.dir.is_some());
        assert!(prog_d.file.is_none());
    }
    fn settings_with(mode: &str, owner: Option<&str>, group: Option<&str>) -> Settings {
        Settings {
            mode: if mode.is_empty() {
                ModeProgram::default()
            } else {
                parse_mode_dsl(mode).unwrap()
            },
            owner: owner
                .map(|s| parse_owner_dsl(s, IdKind::User, &GetentResolver::default()).unwrap())
                .unwrap_or_default(),
            group: group
                .map(|s| parse_owner_dsl(s, IdKind::Group, &GetentResolver::default()).unwrap())
                .unwrap_or_default(),
            fail_early: false,
            defer_dir_changes: false,
            filter: None,
            time_filter: None,
            dry_run: None,
        }
    }
    #[test]
    fn plan_noop_when_already_correct() {
        let s = settings_with("g+r", None, None);
        // file already group-readable
        let plan = compute_plan(0o644, 1000, 1000, EntryKind::File, &s);
        assert!(plan.is_noop());
    }
    #[test]
    fn plan_chmod_when_mode_differs() {
        let s = settings_with("g+w", None, None);
        let plan = compute_plan(0o644, 1000, 1000, EntryKind::File, &s);
        assert_eq!(plan.chmod, Some(0o664));
        assert!(plan.chown.is_none());
    }
    #[test]
    fn plan_chown_only_changed_ids() {
        let s = settings_with("", None, Some("2000"));
        let plan = compute_plan(0o644, 1000, 1000, EntryKind::File, &s);
        // gid changes 1000 -> 2000, uid untouched
        assert_eq!(plan.chown, Some((None, Some(2000))));
        assert!(plan.chmod.is_none());
    }
    #[test]
    fn plan_preserves_setgid_across_chgrp() {
        // file is setgid (0o2755), only --group given; chown clears setgid, so we
        // must re-chmod to the original mode to keep it.
        let s = settings_with("", None, Some("2000"));
        let plan = compute_plan(0o2755, 1000, 1000, EntryKind::File, &s);
        assert_eq!(plan.chown, Some((None, Some(2000))));
        assert_eq!(plan.chmod, Some(0o2755));
    }
    #[test]
    fn plan_symlink_never_chmods_but_chowns() {
        let s = settings_with("g+w", None, Some("2000"));
        let plan = compute_plan(0o777, 1000, 1000, EntryKind::Symlink, &s);
        assert!(plan.chmod.is_none());
        assert_eq!(plan.chown, Some((None, Some(2000))));
    }
    #[test]
    fn plan_preserves_setuid_when_mode_rule_noop_but_chown_runs() {
        // g+r is a no-op on 0o4755 (group already readable), but the chown clears
        // setuid, so the mode-rule branch must still emit a chmod to restore it.
        let s = settings_with("g+r", Some("2000"), None);
        let plan = compute_plan(0o4755, 1000, 1000, EntryKind::File, &s);
        assert_eq!(plan.chown, Some((Some(2000), None)));
        assert_eq!(plan.chmod, Some(0o4755));
    }
    #[test]
    fn plan_preserves_setgid_dir_across_chgrp() {
        // setgid dir, only --group given; chown clears setgid so chmod restores it.
        let s = settings_with("", None, Some("2000"));
        let plan = compute_plan(0o2770, 1000, 1000, EntryKind::Dir, &s);
        assert_eq!(plan.chown, Some((None, Some(2000))));
        assert_eq!(plan.chmod, Some(0o2770));
    }

    static RACE_PROGRESS: std::sync::LazyLock<Progress> = std::sync::LazyLock::new(Progress::new);

    // Repeatedly swap `dir/entry` between a real regular file (mode 0644) and a symlink
    // pointing at `sentinel`, using rename so each individual state is atomic. Two staging
    // names live alongside `entry` and are renamed over it in a tight loop until `stop` is
    // set. Runs on a dedicated OS thread so it makes progress regardless of the tokio
    // runtime's scheduling. Mirrors copy's `spawn_file_symlink_swapper`.
    fn spawn_file_symlink_swapper(
        dir: std::path::PathBuf,
        entry_name: &'static str,
        sentinel: std::path::PathBuf,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        use std::os::unix::fs::PermissionsExt;
        std::thread::spawn(move || {
            let entry = dir.join(entry_name);
            let staged_real = dir.join("__staged_real");
            let staged_link = dir.join("__staged_link");
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                // prepare a real file (mode 0644) at a staging name, then rename it over `entry`.
                let _ = std::fs::remove_file(&staged_real);
                if std::fs::write(&staged_real, b"REAL").is_err() {
                    continue;
                }
                let _ =
                    std::fs::set_permissions(&staged_real, std::fs::Permissions::from_mode(0o644));
                let _ = std::fs::rename(&staged_real, &entry);
                // prepare a symlink-to-sentinel at the other staging name, then rename it over.
                let _ = std::fs::remove_file(&staged_link);
                let _ = std::os::unix::fs::symlink(&sentinel, &staged_link);
                let _ = std::fs::rename(&staged_link, &entry);
            }
        })
    }

    // TOCTOU race: while rchm runs `--mode g+w` over a directory, the entry inside it is
    // rapidly flipped between a real file (0644) and a symlink to a SENTINEL file that lives
    // OUTSIDE the tree with a distinctive mode (0600). rchm classifies each entry through an
    // O_PATH/O_NOFOLLOW handle and chmods that exact pinned inode — so it may chmod the real
    // file (0644 -> 0664), or see a symlink (which it never chmods), or fail closed, but it
    // must NEVER follow the link and change the sentinel's mode. The sentinel staying 0600 on
    // every iteration is the safety assertion. Also confirms the run terminates (timeout).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn entry_symlink_swap_never_changes_sentinel_mode() -> anyhow::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let tmp = crate::testutils::create_temp_dir().await?;
        let root = tmp.as_path();
        // sentinel lives OUTSIDE the rchm target tree, with a distinctive mode we must not touch.
        let sentinel = root.join("sentinel_secret");
        tokio::fs::write(&sentinel, b"SENTINEL").await?;
        std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o600))?;
        let sentinel_uid_before =
            std::os::unix::fs::MetadataExt::uid(&std::fs::symlink_metadata(&sentinel)?);
        // the tree rchm operates on: a directory containing the entry that gets swapped.
        let target_dir = root.join("tree");
        tokio::fs::create_dir(&target_dir).await?;
        let entry_path = target_dir.join("entry");
        tokio::fs::write(&entry_path, b"REAL").await?;
        std::fs::set_permissions(&entry_path, std::fs::Permissions::from_mode(0o644))?;

        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let swapper =
            spawn_file_symlink_swapper(target_dir.clone(), "entry", sentinel.clone(), stop.clone());

        // g+w on a file would turn the sentinel 0600 -> 0620 if the link were ever followed.
        let settings = settings_with("g+w", None, None);
        let mut caught = 0usize;
        let mut changed_real = 0usize;
        for i in 0..300 {
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                chmod(&RACE_PROGRESS, &target_dir, &settings),
            )
            .await
            .expect("rchm must not hang under concurrent swapping");
            match result {
                Ok(summary) => changed_real += summary.files_changed,
                Err(_) => caught += 1, // a swap was caught mid-run (failed closed for that entry)
            }
            // CORE SAFETY ASSERTION (holds on every iteration regardless of timing): the
            // out-of-tree sentinel's mode and owner are never modified by rchm.
            let sentinel_md = std::fs::symlink_metadata(&sentinel)?;
            assert_eq!(
                sentinel_md.permissions().mode() & 0o7777,
                0o600,
                "iteration {i}: sentinel mode changed — rchm followed the symlink to chmod it"
            );
            assert_eq!(
                std::os::unix::fs::MetadataExt::uid(&sentinel_md),
                sentinel_uid_before,
                "iteration {i}: sentinel owner changed — rchm followed the symlink to chown it"
            );
        }

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        swapper.join().expect("swapper thread panicked");
        // sanity (not the safety assertion): the run did observable work across the iterations.
        tracing::info!("entry swap: caught={caught}, changed_real={changed_real}");
        assert!(
            caught + changed_real > 0,
            "expected at least one observable outcome across the iterations"
        );
        Ok(())
    }

    /// Stress tests exercising `pending_meta` (max-open-files) saturation during rchm. The module
    /// name carries the `max_open_files` substring so nextest's serial test-group isolates these
    /// from anything else that mutates the process-wide throttle limit (see `.config/nextest.toml`).
    mod max_open_files_tests {
        use super::*;
        use crate::walk_driver::process_entry;

        static PROGRESS: std::sync::LazyLock<Progress> = std::sync::LazyLock::new(Progress::new);

        /// Regression for the hold-and-wait deadlock when a getdents leaf-hint entry is actually a
        /// directory (the DT_UNKNOWN edge, or a getdents-vs-`child()` swap). A `pending_meta` permit
        /// is pre-acquired for hinted leaves only; if such an entry is really a directory, the
        /// permit must be DROPPED before recursing, or it is held while its children block acquiring
        /// one and a saturated pool hangs the walk.
        ///
        /// Reproduced deterministically by driving a directory entry through the driver's
        /// [`process_entry`] with the real [`ChmodVisitor`] and the one-and-only permit pre-acquired
        /// (as the spawn loop does for a hinted leaf): with the bug the held permit strands the
        /// child's pre-acquire and the timeout fires; with the driver's single drop-before-recurse
        /// site the child acquires it and the walk completes. (Runs the real `ChmodVisitor` through
        /// the driver — the chmod-side coverage the old direct `chmod_entry` call gave, now that
        /// `chmod_entry` no longer exists after the Phase E conversion.)
        #[tokio::test]
        async fn hinted_leaf_that_is_dir_drops_permit_before_recursion() -> anyhow::Result<()> {
            let root = crate::testutils::create_temp_dir().await?;
            // `d` is a directory (the authoritative type) holding one child file `c`.
            let dir_path = root.join("d");
            tokio::fs::create_dir(&dir_path).await?;
            let child_path = dir_path.join("c");
            tokio::fs::write(&child_path, b"x").await?;
            std::fs::set_permissions(&child_path, std::fs::Permissions::from_mode(0o644))?;
            // size the pending-meta pool to a single permit so a held-across-recursion permit
            // strands the child's pre-acquire — the saturation the fd-walk must tolerate.
            throttle::set_max_open_files(1);
            // open the container of `d` and classify `d` itself: an authoritative directory handle.
            let parent = Dir::open_parent_dir(&root, congestion::Side::Destination)
                .await
                .context("open parent dir")?;
            let parent = Arc::new(parent.into_tree());
            let name = std::ffi::OsStr::new("d");
            let handle = parent.child(name).await.context("classify d")?;
            assert_eq!(
                handle.kind(),
                EntryKind::Dir,
                "fixture `d` must be a directory"
            );
            drop(handle);
            // build the visitor + root context for `d`, pre-acquire the single permit as the spawn
            // loop does for a hinted leaf, and hand it to `process_entry` (the fix drops it before
            // recursing).
            let visitor = Arc::new(ChmodVisitor {
                prog_track: &PROGRESS,
                settings: settings_with("g+w", None, None),
            });
            let cx = EntryCx {
                parent: Arc::clone(&parent),
                name: name.to_owned(),
                rel_path: PathBuf::new(),
                filter_path: PathBuf::new(),
                real_path: dir_path.clone(),
                dry_run: false,
                prog_track: &PROGRESS,
            };
            let permit = crate::walk::preacquire_leaf_permit(
                PermitKind::PendingMeta,
                Some(EntryKind::File),
                |_| true,
            )
            .await;
            assert!(permit.is_some(), "the pre-acquire must take the one permit");
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(20),
                process_entry(visitor, cx, (), permit),
            )
            .await;
            // restore the default (disabled) pool before asserting so a failure here can't strand
            // the tiny limit for any concurrent test (the serial group already isolates us, but
            // this keeps the process-global knob clean on the failure path too).
            throttle::set_max_open_files(0);
            let summary = result
                .context(
                    "process_entry hung — leaf permit held across directory recursion (deadlock)",
                )?
                .map_err(|e| e.source)
                .context("process_entry failed")?;
            // both the directory and its child get g+w (0644 -> 0664 on the file).
            assert_eq!(
                summary.files_changed, 1,
                "child file should have its mode changed"
            );
            assert_eq!(
                summary.directories_changed, 1,
                "directory should have its mode changed"
            );
            assert_eq!(
                std::fs::symlink_metadata(&child_path)?.permissions().mode() & 0o777,
                0o664,
                "child file mode should be g+w applied"
            );
            Ok(())
        }
    }
}
