//! Recursive permission/ownership changes (chmod/chgrp/chown) over a fileset.
//!
//! The public entry point is [`chmod`]; it mirrors [`crate::rm()`] but transforms
//! metadata in place (from a per-type rule) instead of removing entries.
use crate::filter::TimeFilter;
use crate::progress::Progress;
use crate::walk::{self, EntryKind};
use anyhow::{Context, anyhow};
use async_recursion::async_recursion;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use tracing::instrument;

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

/// Resolve a DSL id token to a numeric id. All-numeric tokens are used directly;
/// otherwise the token is looked up as a user/group name (matching `chown`/`chgrp`).
fn resolve_id(token: &str, kind: IdKind) -> anyhow::Result<u32> {
    if let Ok(n) = token.parse::<u32>() {
        return Ok(n);
    }
    match kind {
        IdKind::User => nix::unistd::User::from_name(token)
            .with_context(|| format!("looking up user {token:?}"))?
            .map(|u| u.uid.as_raw())
            .ok_or_else(|| anyhow!("unknown user: {token}")),
        IdKind::Group => nix::unistd::Group::from_name(token)
            .with_context(|| format!("looking up group {token:?}"))?
            .map(|g| g.gid.as_raw())
            .ok_or_else(|| anyhow!("unknown group: {token}")),
    }
}

/// Parse a `--group`/`--owner` DSL string. A bare token is the default for all
/// types; `f:`/`d:`/`l:` sections override per type.
pub fn parse_owner_dsl(s: &str, kind: IdKind) -> anyhow::Result<OwnerProgram> {
    let mut prog = OwnerProgram::default();
    let mut bare: Option<u32> = None;
    for clause in s.split_whitespace() {
        if let Some((ty, rest)) = clause.split_once(':') {
            let id = resolve_id(rest, kind)?;
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
            bare = Some(resolve_id(clause, kind)?);
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

async fn apply_plan(path: &std::path::Path, plan: &EntryPlan) -> anyhow::Result<()> {
    if let Some((uid, gid)) = plan.chown {
        let dst = path.to_owned();
        walk::run_metadata_probed(
            congestion::Side::Destination,
            congestion::MetadataOp::Chmod,
            async {
                tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
                    nix::unistd::fchownat(
                        nix::fcntl::AT_FDCWD,
                        &dst,
                        uid.map(Into::into),
                        gid.map(Into::into),
                        nix::fcntl::AtFlags::AT_SYMLINK_NOFOLLOW,
                    )
                    .with_context(|| format!("failed to chown {dst:?}"))?;
                    Ok(())
                })
                .await?
            },
        )
        .await?;
    }
    if let Some(mode) = plan.chmod {
        // path-based chmod (only ever called on non-symlinks); follows the entry
        // itself. avoids an extra open() vs fchmod and matches rm's dir path.
        walk::run_metadata_probed(
            congestion::Side::Destination,
            congestion::MetadataOp::Chmod,
            tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)),
        )
        .await
        .with_context(|| format!("failed to chmod {path:?} to {mode:04o}"))?;
    }
    Ok(())
}

/// Apply the change to a single entry (a leaf, or a directory after its children
/// were processed). Handles the time filter, dry-run, no-op skip, and counters.
async fn process_entry(
    prog: &'static Progress,
    path: &std::path::Path,
    metadata: &std::fs::Metadata,
    kind: EntryKind,
    settings: &Settings,
) -> Result<Summary, Error> {
    if let Some(ref time_filter) = settings.time_filter {
        match time_filter.matches(metadata) {
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
    let plan = compute_plan(
        metadata.mode(),
        metadata.uid(),
        metadata.gid(),
        kind,
        settings,
    );
    if plan.is_noop() {
        if let Some(crate::config::DryRunMode::All) = settings.dry_run {
            println!("unchanged {} {:?}", kind.label(), path);
        }
        return Ok(inc_unchanged(prog, kind));
    }
    if settings.dry_run.is_some() {
        let desc = describe_change(metadata.mode(), metadata.uid(), metadata.gid(), &plan);
        println!("would modify {} {:?}: {}", kind.label(), path, desc);
        return Ok(inc_changed(prog, kind));
    }
    apply_plan(path, &plan)
        .await
        .map_err(|err| Error::new(err, Default::default()))?;
    Ok(inc_changed(prog, kind))
}

/// Strip trailing path separators from a root operand. A trailing slash forces the
/// OS to resolve the final component as a directory, which would dereference a symlink
/// root like `link/` (following it to its target) -- violating the non-dereference
/// behavior. Stripping makes `link/` behave like `link` (the symlink itself).
/// (This non-dereference behavior is not robust against concurrent path replacement
/// under elevated privilege; see `docs/tocttou.md`.)
fn without_trailing_separators(path: &std::path::Path) -> std::path::PathBuf {
    use std::os::unix::ffi::OsStrExt;
    let bytes = path.as_os_str().as_bytes();
    let mut end = bytes.len();
    while end > 1 && bytes[end - 1] == b'/' {
        end -= 1;
    }
    std::path::PathBuf::from(std::ffi::OsStr::from_bytes(&bytes[..end]))
}

/// Public entry point. Applies metadata changes to `path` and, recursively, its
/// contents. Mirrors [`crate::rm::rm`] for the root-filter check.
#[instrument(skip(prog_track, settings))]
pub async fn chmod(
    prog_track: &'static Progress,
    path: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    let stripped = without_trailing_separators(path);
    let path = stripped.as_path();
    if let Some(ref filter) = settings.filter
        && let Some(name) = path.file_name().map(std::path::Path::new)
    {
        let metadata = walk::run_metadata_probed(
            congestion::Side::Source,
            congestion::MetadataOp::Stat,
            tokio::fs::symlink_metadata(path),
        )
        .await
        .with_context(|| format!("failed reading metadata from {path:?}"))
        .map_err(|err| Error::new(err, Default::default()))?;
        match filter.should_include_root_item(name, metadata.is_dir()) {
            crate::filter::FilterResult::Included => {}
            result => {
                let kind = EntryKind::from_metadata(&metadata);
                if let Some(mode) = settings.dry_run {
                    crate::dry_run::report_skip(path, &result, mode, kind.label_long());
                }
                kind.inc_skipped(prog_track);
                return Ok(skipped_summary_for(kind));
            }
        }
    }
    chmod_internal(prog_track, path, path, settings).await
}

/// Apply the directory's own change, or skip it when the directory was only traversed to
/// find include-matches. Factored out so the walk can run it before (pre-order, default)
/// or after (post-order, `--defer-dir-changes`) descending into the contents.
async fn apply_dir_self(
    prog_track: &'static Progress,
    path: &std::path::Path,
    metadata: &std::fs::Metadata,
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
    process_entry(prog_track, path, metadata, EntryKind::Dir, settings).await
}

#[instrument(skip(prog_track, settings))]
#[async_recursion]
async fn chmod_internal(
    prog_track: &'static Progress,
    path: &std::path::Path,
    source_root: &std::path::Path,
    settings: &Settings,
) -> Result<Summary, Error> {
    let _ops_guard = prog_track.ops.guard();
    let metadata = walk::run_metadata_probed(
        congestion::Side::Source,
        congestion::MetadataOp::Stat,
        tokio::fs::symlink_metadata(path),
    )
    .await
    .with_context(|| format!("failed reading metadata from {path:?}"))
    .map_err(|err| Error::new(err, Default::default()))?;
    let kind = EntryKind::from_metadata(&metadata);
    if kind != EntryKind::Dir {
        return process_entry(prog_track, path, &metadata, kind, settings).await;
    }
    // a directory may have been entered only because it *could contain* include-matches,
    // not because it directly matches an include pattern. such "traversed-only" dirs are
    // not modified themselves (mirrors rm.rs) -- only entries the filter directly selects.
    let relative_path = path.strip_prefix(source_root).unwrap_or(path);
    let traversed_only = settings
        .filter
        .as_ref()
        .is_some_and(|f| f.has_includes() && !f.directly_matches_include(relative_path, true));
    let errors = crate::error_collector::ErrorCollector::default();
    let mut summary = Summary::default();
    // pre-order (default, like `chmod -R`): change the directory BEFORE descending, so
    // `--mode d:u+rwx` can recover an unreadable directory and a child failure can't
    // prevent the directory's own change.
    if !settings.defer_dir_changes {
        match apply_dir_self(prog_track, path, &metadata, traversed_only, settings).await {
            Ok(dir_summary) => summary = summary + dir_summary,
            Err(error) => {
                if settings.fail_early {
                    return Err(Error::new(error.source, summary + error.summary));
                }
                tracing::error!("chmod: {:?} failed with: {:#}", path, &error);
                summary = summary + error.summary;
                errors.push(error.source);
            }
        }
    }
    // descend into the directory's contents
    match tokio::fs::read_dir(path).await {
        Ok(mut entries) => {
            let mut join_set = tokio::task::JoinSet::new();
            loop {
                let (entry, entry_file_type) =
                    match walk::next_entry_probed(&mut entries, congestion::Side::Source, || {
                        format!("failed traversing directory {path:?}")
                    })
                    .await
                    {
                        Ok(Some(entry)) => entry,
                        Ok(None) => break,
                        Err(error) => {
                            if settings.fail_early {
                                return Err(Error::new(error, summary));
                            }
                            tracing::error!("chmod: {:#}", &error);
                            errors.push(error);
                            break;
                        }
                    };
                let entry_path = entry.path();
                let entry_kind = EntryKind::from_file_type(entry_file_type.as_ref());
                let relative_path = entry_path.strip_prefix(source_root).unwrap_or(&entry_path);
                if let Some(skip_result) = walk::should_skip_entry(
                    &settings.filter,
                    relative_path,
                    entry_kind == EntryKind::Dir,
                ) {
                    if let Some(mode) = settings.dry_run {
                        crate::dry_run::report_skip(
                            &entry_path,
                            &skip_result,
                            mode,
                            entry_kind.label(),
                        );
                    }
                    entry_kind.inc_skipped(prog_track);
                    summary = summary + skipped_summary_for(entry_kind);
                    continue;
                }
                let settings = settings.clone();
                let source_root = source_root.to_owned();
                let known_leaf = entry_file_type.as_ref().is_some_and(|ft| !ft.is_dir());
                let pending_guard = if known_leaf {
                    Some(throttle::pending_meta_permit().await)
                } else {
                    None
                };
                join_set.spawn(async move {
                    let _pending_guard = pending_guard;
                    chmod_internal(prog_track, &entry_path, &source_root, &settings).await
                });
            }
            drop(entries);
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(Ok(child)) => summary = summary + child,
                    Ok(Err(error)) => {
                        tracing::error!("chmod: {:?} failed with: {:#}", path, &error);
                        summary = summary + error.summary;
                        errors.push(error.source);
                        if settings.fail_early {
                            break;
                        }
                    }
                    Err(error) => {
                        errors.push(error.into());
                        if settings.fail_early {
                            break;
                        }
                    }
                }
            }
        }
        Err(read_error) => {
            // couldn't read the directory -- e.g. owner r/x was just removed (a pre-order
            // restrictive change) or it was already unreadable with no traversability-
            // restoring rule. report it; unless --fail-early, keep going with the rest.
            let error = anyhow::Error::new(read_error)
                .context(format!("failed reading directory {path:?}"));
            if settings.fail_early {
                return Err(Error::new(error, summary));
            }
            tracing::error!("chmod: {:#}", &error);
            errors.push(error);
        }
    }
    // under --fail-early, a child failure broke the join loop above; stop here, before any
    // deferred parent change -- never apply more changes after the error we were asked to
    // stop on. (read_dir / next_entry failures already returned directly.)
    if settings.fail_early && errors.has_errors() {
        return Err(Error::new(errors.into_error().unwrap(), summary));
    }
    // post-order (`--defer-dir-changes`): change the directory AFTER its contents -- needed
    // when recursively removing the owner's own traversal permission. in keep-going mode it
    // is applied even after a child failure; fail-early returned just above.
    if settings.defer_dir_changes {
        match apply_dir_self(prog_track, path, &metadata, traversed_only, settings).await {
            Ok(dir_summary) => summary = summary + dir_summary,
            Err(error) => {
                if settings.fail_early {
                    return Err(Error::new(error.source, summary + error.summary));
                }
                tracing::error!("chmod: {:?} failed with: {:#}", path, &error);
                summary = summary + error.summary;
                errors.push(error.source);
            }
        }
    }
    if errors.has_errors() {
        return Err(Error::new(errors.into_error().unwrap(), summary));
    }
    Ok(summary)
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
        let prog = parse_owner_dsl("0", IdKind::User).unwrap();
        assert_eq!(prog.file, Some(0));
        assert_eq!(prog.dir, Some(0));
        assert_eq!(prog.symlink, Some(0));
    }
    #[test]
    fn owner_dsl_per_type_overrides() {
        let prog = parse_owner_dsl("f:1 d:2", IdKind::User).unwrap();
        assert_eq!(prog.file, Some(1));
        assert_eq!(prog.dir, Some(2));
        assert_eq!(prog.symlink, None);
    }
    #[test]
    fn owner_dsl_bare_plus_override() {
        let prog = parse_owner_dsl("5 d:2", IdKind::Group).unwrap();
        assert_eq!(prog.file, Some(5));
        assert_eq!(prog.dir, Some(2));
        assert_eq!(prog.symlink, Some(5));
    }
    #[test]
    fn owner_dsl_explicit_before_bare_is_order_independent() {
        let prog = parse_owner_dsl("f:1 5", IdKind::User).unwrap();
        assert_eq!(prog.file, Some(1));
        assert_eq!(prog.dir, Some(5));
        assert_eq!(prog.symlink, Some(5));
    }
    #[test]
    fn owner_dsl_rejects_multiple_bare() {
        assert!(parse_owner_dsl("1 2", IdKind::User).is_err());
    }
    #[test]
    fn owner_dsl_rejects_unknown_id() {
        assert!(parse_owner_dsl("definitely-no-such-group-xyz", IdKind::Group).is_err());
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
                .map(|s| parse_owner_dsl(s, IdKind::User).unwrap())
                .unwrap_or_default(),
            group: group
                .map(|s| parse_owner_dsl(s, IdKind::Group).unwrap())
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
}
