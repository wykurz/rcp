use anyhow::{Context, Result};
use async_recursion::async_recursion;
use enum_map::{Enum, EnumMap};
use tokio::io::AsyncWriteExt;
use tracing::instrument;

use crate::copy::is_file_type_same;
use crate::filecmp;
use crate::progress;

#[derive(Copy, Clone, Debug, Enum)]
pub enum CompareResult {
    Same,
    Different,
    SrcMissing, // object missing in src but present in dst
    DstMissing, // same as above but flipped
}

#[derive(Copy, Clone, Debug, Enum)]
pub enum ObjType {
    File,
    Dir,
    Symlink,
    Other, // sockets, block devices, character devices, FIFOs, etc.
}

pub type ObjSettings = EnumMap<ObjType, filecmp::MetadataCmpSettings>;

#[derive(Debug, Clone)]
pub struct Settings {
    pub compare: ObjSettings,
    pub fail_early: bool,
    pub exit_early: bool,
    pub expand_missing: bool,
    pub filter: Option<crate::filter::FilterSettings>,
}

pub type Mismatch = EnumMap<ObjType, EnumMap<CompareResult, u64>>;

/// Count of skipped items per object type
pub type Skipped = EnumMap<ObjType, u64>;

/// Output format for comparison results and summary.
#[derive(Copy, Clone, Debug, Default, clap::ValueEnum)]
pub enum OutputFormat {
    /// JSON output (NDJSON for differences, JSON object for summary)
    #[default]
    Json,
    /// Human-readable text output (legacy format)
    Text,
}

fn compare_result_name(cr: CompareResult) -> &'static str {
    match cr {
        CompareResult::Same => "same",
        CompareResult::Different => "different",
        CompareResult::SrcMissing => "src_missing",
        CompareResult::DstMissing => "dst_missing",
    }
}

fn obj_type_name(ot: ObjType) -> &'static str {
    match ot {
        ObjType::File => "file",
        ObjType::Dir => "dir",
        ObjType::Symlink => "symlink",
        ObjType::Other => "other",
    }
}

/// Encodes a path as a JSON-safe string that is round-trippable for arbitrary
/// Unix paths. Literal backslashes are escaped as `\\`, and non-UTF-8 bytes
/// are escaped as `\xHH`. To decode, first parse the JSON string, then scan
/// left-to-right: `\\` → literal `\`, `\xHH` → raw byte, all other characters
/// are literal UTF-8.
fn path_to_json_string(path: &std::path::Path) -> String {
    use std::os::unix::ffi::OsStrExt;
    let bytes = path.as_os_str().as_bytes();
    let mut out = String::with_capacity(bytes.len());
    for chunk in bytes.utf8_chunks() {
        for c in chunk.valid().chars() {
            if c == '\\' {
                out.push_str("\\\\");
            } else {
                out.push(c);
            }
        }
        for &b in chunk.invalid() {
            use std::fmt::Write;
            write!(out, "\\x{b:02x}").unwrap();
        }
    }
    out
}

#[derive(Default)]
pub struct Summary {
    pub mismatch: Mismatch,
    pub skipped: Skipped,
    /// Total size of regular files compared on the source side, in bytes.
    pub src_bytes: u64,
    /// Total size of regular files compared on the destination side, in bytes.
    pub dst_bytes: u64,
}

impl std::ops::Add for Summary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        let mut mismatch = self.mismatch;
        for (obj_type, &cmp_res_map) in &other.mismatch {
            for (cmp_res, &count) in &cmp_res_map {
                mismatch[obj_type][cmp_res] += count;
            }
        }
        let mut skipped = self.skipped;
        for (obj_type, &count) in &other.skipped {
            skipped[obj_type] += count;
        }
        Self {
            mismatch,
            skipped,
            src_bytes: self.src_bytes + other.src_bytes,
            dst_bytes: self.dst_bytes + other.dst_bytes,
        }
    }
}

impl std::fmt::Display for Summary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(
            f,
            "src size (compared): {}",
            bytesize::ByteSize(self.src_bytes)
        )?;
        writeln!(
            f,
            "dst size (compared): {}",
            bytesize::ByteSize(self.dst_bytes)
        )?;
        for (obj_type, &cmp_res_map) in &self.mismatch {
            for (cmp_res, &count) in &cmp_res_map {
                writeln!(f, "{obj_type:?} {cmp_res:?}: {count}")?;
            }
        }
        for (obj_type, &count) in &self.skipped {
            if count > 0 {
                writeln!(f, "{obj_type:?} Skipped: {count}")?;
            }
        }
        Ok(())
    }
}

/// Wraps a [`Summary`] with an [`OutputFormat`] so that [`Display`](std::fmt::Display)
/// renders either human-readable text or JSON.
pub struct FormattedSummary {
    pub summary: Summary,
    pub format: OutputFormat,
}

impl std::fmt::Display for FormattedSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.format {
            OutputFormat::Text => write!(f, "{}", self.summary),
            OutputFormat::Json => {
                let mut mismatch = serde_json::Map::new();
                for (obj_type, &cmp_res_map) in &self.summary.mismatch {
                    let mut counts = serde_json::Map::new();
                    for (cmp_res, &count) in &cmp_res_map {
                        counts.insert(
                            compare_result_name(cmp_res).to_string(),
                            serde_json::Value::Number(count.into()),
                        );
                    }
                    mismatch.insert(
                        obj_type_name(obj_type).to_string(),
                        serde_json::Value::Object(counts),
                    );
                }
                let mut skipped = serde_json::Map::new();
                for (obj_type, &count) in &self.summary.skipped {
                    if count > 0 {
                        skipped.insert(
                            obj_type_name(obj_type).to_string(),
                            serde_json::Value::Number(count.into()),
                        );
                    }
                }
                let stats = crate::collect_runtime_stats();
                let walltime = crate::get_progress().get_duration();
                let obj = serde_json::json!({
                    "src_bytes": self.summary.src_bytes,
                    "dst_bytes": self.summary.dst_bytes,
                    "mismatch": serde_json::Value::Object(mismatch),
                    "skipped": serde_json::Value::Object(skipped),
                    "walltime_ms": walltime.as_millis() as u64,
                    "cpu_time_user_ms": stats.cpu_time_user_ms,
                    "cpu_time_kernel_ms": stats.cpu_time_kernel_ms,
                    "peak_rss_bytes": stats.peak_rss_bytes,
                });
                write!(f, "{obj}")
            }
        }
    }
}

#[derive(Clone)]
pub struct LogWriter {
    file: Option<std::sync::Arc<tokio::sync::Mutex<tokio::io::BufWriter<tokio::fs::File>>>>,
    stdout: Option<std::sync::Arc<tokio::sync::Mutex<tokio::io::BufWriter<tokio::io::Stdout>>>>,
    format: OutputFormat,
}

impl std::fmt::Debug for LogWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogWriter")
            .field("file", &self.file.is_some())
            .field("stdout", &self.stdout.is_some())
            .field("format", &self.format)
            .finish()
    }
}

impl LogWriter {
    /// Creates a new LogWriter.
    ///
    /// If `log_path_opt` is provided, output goes to that file.
    /// Otherwise, if `use_stdout` is true, output goes to stdout.
    /// If both are false/None, no output is produced.
    pub async fn new(
        log_path_opt: Option<&std::path::Path>,
        use_stdout: bool,
        format: OutputFormat,
    ) -> Result<Self> {
        if let Some(log_path) = log_path_opt {
            let log_file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(log_path)
                .await
                .with_context(|| format!("Failed to open log file: {log_path:?}"))?;
            let log =
                std::sync::Arc::new(tokio::sync::Mutex::new(tokio::io::BufWriter::new(log_file)));
            Ok(Self {
                file: Some(log),
                stdout: None,
                format,
            })
        } else if use_stdout {
            Ok(Self {
                file: None,
                stdout: Some(std::sync::Arc::new(tokio::sync::Mutex::new(
                    tokio::io::BufWriter::new(tokio::io::stdout()),
                ))),
                format,
            })
        } else {
            Ok(Self {
                file: None,
                stdout: None,
                format,
            })
        }
    }
    /// Creates a silent LogWriter that produces no output, using the default format.
    /// Convenience constructor primarily for tests.
    pub async fn silent() -> Result<Self> {
        Self::new(None, false, OutputFormat::default()).await
    }

    pub async fn log_mismatch(
        &self,
        cmp_result: CompareResult,
        src_obj_type: Option<ObjType>,
        src: &std::path::Path,
        dst_obj_type: Option<ObjType>,
        dst: &std::path::Path,
    ) -> Result<()> {
        let msg = match self.format {
            OutputFormat::Text => {
                format!(
                    "[{cmp_result:?}]\n\t[{src_obj_type:?}]\t{src:?}\n\t[{dst_obj_type:?}]\t{dst:?}\n"
                )
            }
            OutputFormat::Json => {
                let src_type_val = match src_obj_type {
                    Some(ot) => serde_json::Value::String(obj_type_name(ot).to_string()),
                    None => serde_json::Value::Null,
                };
                let dst_type_val = match dst_obj_type {
                    Some(ot) => serde_json::Value::String(obj_type_name(ot).to_string()),
                    None => serde_json::Value::Null,
                };
                let obj = serde_json::json!({
                    "result": compare_result_name(cmp_result),
                    "src_type": src_type_val,
                    "src": path_to_json_string(src),
                    "dst_type": dst_type_val,
                    "dst": path_to_json_string(dst),
                });
                format!("{obj}\n")
            }
        };
        self.write(&msg).await
    }

    async fn write(&self, msg: &str) -> Result<()> {
        if let Some(log) = &self.file {
            let mut log = log.lock().await;
            log.write_all(msg.as_bytes())
                .await
                .context("Failed to write to log file")?;
        }
        if let Some(stdout) = &self.stdout {
            let mut stdout = stdout.lock().await;
            stdout
                .write_all(msg.as_bytes())
                .await
                .context("Failed to write to stdout")?;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        if let Some(log) = &self.file {
            let mut log = log.lock().await;
            log.flush().await.context("Failed to flush log file")?;
        }
        if let Some(stdout) = &self.stdout {
            let mut stdout = stdout.lock().await;
            stdout.flush().await.context("Failed to flush stdout")?;
        }
        Ok(())
    }
}

fn obj_type(metadata: &std::fs::Metadata) -> ObjType {
    if metadata.is_file() {
        ObjType::File
    } else if metadata.is_dir() {
        ObjType::Dir
    } else if metadata.is_symlink() {
        ObjType::Symlink
    } else {
        // sockets, block devices, character devices, FIFOs, etc.
        ObjType::Other
    }
}

/// Public entry point for compare operations.
/// Internally delegates to cmp_internal with source_root/dest_root tracking for proper filter matching.
#[instrument(skip(prog_track))]
pub async fn cmp(
    prog_track: &'static progress::Progress,
    src: &std::path::Path,
    dst: &std::path::Path,
    log: &LogWriter,
    settings: &Settings,
) -> Result<Summary> {
    cmp_internal(prog_track, src, dst, src, dst, log, settings).await
}

/// Recursively walks a directory tree on the existing side and records every entry as missing
/// on the other side.
#[instrument(skip(prog_track))]
#[async_recursion]
async fn expand_missing_tree(
    prog_track: &'static progress::Progress,
    existing_path: &std::path::Path,
    mirror_path: &std::path::Path,
    existing_root: &std::path::Path,
    result: CompareResult,
    log: &LogWriter,
    settings: &Settings,
) -> Result<Summary> {
    let _prog_guard = prog_track.ops.guard();
    let metadata = tokio::fs::symlink_metadata(existing_path)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &existing_path))?;
    let existing_obj_type = obj_type(&metadata);
    let mut summary = Summary::default();
    summary.mismatch[existing_obj_type][result] += 1;
    // track file sizes on the appropriate side
    if metadata.is_file() {
        match result {
            CompareResult::DstMissing => summary.src_bytes += metadata.len(),
            CompareResult::SrcMissing => summary.dst_bytes += metadata.len(),
            _ => {}
        }
    }
    match result {
        CompareResult::DstMissing => {
            log.log_mismatch(
                result,
                Some(existing_obj_type),
                existing_path,
                None,
                mirror_path,
            )
            .await?;
        }
        CompareResult::SrcMissing => {
            log.log_mismatch(
                result,
                None,
                mirror_path,
                Some(existing_obj_type),
                existing_path,
            )
            .await?;
        }
        _ => {}
    }
    if settings.exit_early {
        return Ok(summary);
    }
    if !metadata.is_dir() {
        return Ok(summary);
    }
    let mut entries = tokio::fs::read_dir(existing_path)
        .await
        .with_context(|| format!("cannot open directory {:?} for reading", &existing_path))?;
    let mut join_set = tokio::task::JoinSet::new();
    let errors = crate::error_collector::ErrorCollector::default();
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &existing_path))?
    {
        throttle::get_ops_token().await;
        let entry_path = entry.path();
        let entry_name = entry_path.file_name().unwrap();
        // apply filter if configured
        if let Some(ref filter) = settings.filter {
            let relative_path = entry_path
                .strip_prefix(existing_root)
                .unwrap_or(&entry_path);
            let entry_file_type = entry.file_type().await.ok();
            let is_dir = entry_file_type.map(|ft| ft.is_dir()).unwrap_or(false);
            if !matches!(
                filter.should_include(relative_path, is_dir),
                crate::filter::FilterResult::Included
            ) {
                // increment skipped counter based on entry type
                let entry_obj_type = if is_dir {
                    ObjType::Dir
                } else if entry_file_type.map(|ft| ft.is_symlink()).unwrap_or(false) {
                    ObjType::Symlink
                } else {
                    ObjType::File
                };
                summary.skipped[entry_obj_type] += 1;
                continue;
            }
        }
        let child_mirror = mirror_path.join(entry_name);
        let log = log.clone();
        let settings = settings.clone();
        let existing_root = existing_root.to_owned();
        join_set.spawn(async move {
            expand_missing_tree(
                prog_track,
                &entry_path,
                &child_mirror,
                &existing_root,
                result,
                &log,
                &settings,
            )
            .await
        });
    }
    drop(entries);
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(child_summary) => summary = summary + child_summary,
            Err(error) => {
                tracing::error!(
                    "expand_missing_tree: {:?} failed with: {:#}",
                    existing_path,
                    &error
                );
                errors.push(error);
                if settings.fail_early {
                    break;
                }
            }
        }
    }
    if let Some(err) = errors.into_error() {
        return Err(err);
    }
    Ok(summary)
}

#[instrument(skip(prog_track))]
#[async_recursion]
async fn cmp_internal(
    prog_track: &'static progress::Progress,
    src: &std::path::Path,
    dst: &std::path::Path,
    source_root: &std::path::Path,
    dest_root: &std::path::Path,
    log: &LogWriter,
    settings: &Settings,
) -> Result<Summary> {
    let _prog_guard = prog_track.ops.guard();
    tracing::debug!("reading source metadata");
    // it is impossible for src not exist other than user passing invalid path (which is an error)
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src))?;
    // apply filter to root item (when src == source_root, this is the initial call)
    if src == source_root {
        if let Some(ref filter) = settings.filter {
            if let Some(name) = src.file_name() {
                let is_dir = src_metadata.is_dir();
                if !matches!(
                    filter.should_include_root_item(name.as_ref(), is_dir),
                    crate::filter::FilterResult::Included
                ) {
                    // root item filtered out, return summary with skipped count
                    let src_obj_type = obj_type(&src_metadata);
                    let mut summary = Summary::default();
                    summary.skipped[src_obj_type] += 1;
                    return Ok(summary);
                }
            }
        }
    }
    let mut cmp_summary = Summary::default();
    let src_obj_type = obj_type(&src_metadata);
    // track file sizes for the summary
    if src_metadata.is_file() {
        cmp_summary.src_bytes += src_metadata.len();
    }
    let dst_metadata = {
        match tokio::fs::symlink_metadata(dst).await {
            Ok(metadata) => metadata,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    if settings.expand_missing && src_metadata.is_dir() {
                        let expanded = expand_missing_tree(
                            prog_track,
                            src,
                            dst,
                            source_root,
                            CompareResult::DstMissing,
                            log,
                            settings,
                        )
                        .await?;
                        cmp_summary = cmp_summary + expanded;
                    } else {
                        cmp_summary.mismatch[src_obj_type][CompareResult::DstMissing] += 1;
                        log.log_mismatch(
                            CompareResult::DstMissing,
                            Some(src_obj_type),
                            src,
                            None,
                            dst,
                        )
                        .await?;
                    }
                    return Ok(cmp_summary);
                }
                return Err(err).context(format!("failed reading metadata from {:?}", &dst));
            }
        }
    };
    if dst_metadata.is_file() {
        cmp_summary.dst_bytes += dst_metadata.len();
    }
    if !is_file_type_same(&src_metadata, &dst_metadata)
        || !filecmp::metadata_equal(
            &settings.compare[src_obj_type],
            &src_metadata,
            &dst_metadata,
        )
    {
        // we use the src type for the summary attribution
        cmp_summary.mismatch[src_obj_type][CompareResult::Different] += 1;
        let dst_obj_type = obj_type(&dst_metadata);
        log.log_mismatch(
            CompareResult::Different,
            Some(src_obj_type),
            src,
            Some(dst_obj_type),
            dst,
        )
        .await?;
        if settings.exit_early {
            return Ok(cmp_summary);
        }
    } else {
        cmp_summary.mismatch[src_obj_type][CompareResult::Same] += 1;
    }
    if !src_metadata.is_dir() || !dst_metadata.is_dir() {
        // nothing more to do
        return Ok(cmp_summary);
    }
    tracing::debug!("process contents of 'src' directory");
    let mut src_entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {src:?} for reading"))?;
    let mut join_set = tokio::task::JoinSet::new();
    let errors = crate::error_collector::ErrorCollector::default();
    // create a set of all the files we already processed
    let mut processed_files = std::collections::HashSet::new();
    // iterate through src entries and recursively call "cmp" on each one
    while let Some(src_entry) = src_entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &src))?
    {
        // it's better to await the token here so that we throttle the syscalls generated by the
        // DirEntry call. the ops-throttle will never cause a deadlock (unlike max-open-files limit)
        // so it's safe to do here.
        throttle::get_ops_token().await;
        let entry_path = src_entry.path();
        let entry_name = entry_path.file_name().unwrap();
        // apply filter if configured
        if let Some(ref filter) = settings.filter {
            // compute relative path from source_root for filter matching
            let relative_path = entry_path.strip_prefix(source_root).unwrap_or(&entry_path);
            let entry_file_type = src_entry.file_type().await.ok();
            let is_dir = entry_file_type.map(|ft| ft.is_dir()).unwrap_or(false);
            if !matches!(
                filter.should_include(relative_path, is_dir),
                crate::filter::FilterResult::Included
            ) {
                // increment skipped counter based on entry type
                let entry_obj_type = if is_dir {
                    ObjType::Dir
                } else if entry_file_type.map(|ft| ft.is_symlink()).unwrap_or(false) {
                    ObjType::Symlink
                } else {
                    ObjType::File
                };
                cmp_summary.skipped[entry_obj_type] += 1;
                continue;
            }
        }
        processed_files.insert(entry_name.to_owned());
        let dst_path = dst.join(entry_name);
        let log = log.clone();
        let settings = settings.clone();
        let source_root = source_root.to_owned();
        let dest_root = dest_root.to_owned();
        let do_cmp = || async move {
            cmp_internal(
                prog_track,
                &entry_path,
                &dst_path,
                &source_root,
                &dest_root,
                &log,
                &settings,
            )
            .await
        };
        join_set.spawn(do_cmp());
    }
    // unfortunately ReadDir is opening file-descriptors and there's not a good way to limit this,
    // one thing we CAN do however is to drop it as soon as we're done with it
    drop(src_entries);
    tracing::debug!("process contents of 'dst' directory");
    let mut dst_entries = tokio::fs::read_dir(dst)
        .await
        .with_context(|| format!("cannot open directory {:?} for reading", &dst))?;
    // iterate through update entries and log each one that's not present in src
    while let Some(dst_entry) = dst_entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &dst))?
    {
        let entry_path = dst_entry.path();
        let entry_name = entry_path.file_name().unwrap();
        if processed_files.contains(entry_name) {
            // we already must have considered this file, skip it
            continue;
        }
        // apply filter if configured - if this entry would be filtered, don't report as missing
        if let Some(ref filter) = settings.filter {
            // compute relative path from dest_root for filter matching
            let relative_path = entry_path.strip_prefix(dest_root).unwrap_or(&entry_path);
            let entry_file_type = dst_entry.file_type().await.ok();
            let is_dir = entry_file_type.map(|ft| ft.is_dir()).unwrap_or(false);
            if !matches!(
                filter.should_include(relative_path, is_dir),
                crate::filter::FilterResult::Included
            ) {
                // increment skipped counter based on entry type
                let entry_obj_type = if is_dir {
                    ObjType::Dir
                } else if entry_file_type.map(|ft| ft.is_symlink()).unwrap_or(false) {
                    ObjType::Symlink
                } else {
                    ObjType::File
                };
                cmp_summary.skipped[entry_obj_type] += 1;
                continue;
            }
        }
        tracing::debug!("found a new entry in the 'dst' directory");
        let dst_path = dst.join(entry_name);
        let dst_entry_metadata = tokio::fs::symlink_metadata(&dst_path)
            .await
            .with_context(|| format!("failed reading metadata from {:?}", &dst_path))?;
        let dst_obj_type = obj_type(&dst_entry_metadata);
        if settings.expand_missing && dst_entry_metadata.is_dir() {
            match expand_missing_tree(
                prog_track,
                &dst_path,
                &src.join(entry_name),
                dest_root,
                CompareResult::SrcMissing,
                log,
                settings,
            )
            .await
            {
                Ok(expanded) => cmp_summary = cmp_summary + expanded,
                Err(error) => {
                    tracing::error!(
                        "expand_missing_tree: {:?} failed with: {:#}",
                        &dst_path,
                        &error
                    );
                    errors.push(error);
                    if settings.fail_early {
                        // unwrap is safe: we just pushed an error
                        return Err(errors.into_error().unwrap());
                    }
                }
            }
        } else {
            if dst_entry_metadata.is_file() {
                cmp_summary.dst_bytes += dst_entry_metadata.len();
            }
            cmp_summary.mismatch[dst_obj_type][CompareResult::SrcMissing] += 1;
            log.log_mismatch(
                CompareResult::SrcMissing,
                None,
                &src.join(entry_name),
                Some(dst_obj_type),
                &dst_path,
            )
            .await?;
        }
    }
    // unfortunately ReadDir is opening file-descriptors and there's not a good way to limit this,
    // one thing we CAN do however is to drop it as soon as we're done with it
    drop(dst_entries);
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(summary) => cmp_summary = cmp_summary + summary,
            Err(error) => {
                tracing::error!("cmp: {:?} vs {:?} failed with: {:#}", src, dst, &error);
                errors.push(error);
                if settings.fail_early {
                    break;
                }
            }
        }
    }
    if let Some(err) = errors.into_error() {
        return Err(err);
    }
    Ok(cmp_summary)
}

#[cfg(test)]
mod cmp_tests {
    use crate::copy;
    use crate::preserve;
    use crate::testutils;
    use enum_map::enum_map;
    use tracing_test::traced_test;

    use super::*;

    static PROGRESS: std::sync::LazyLock<progress::Progress> =
        std::sync::LazyLock::new(progress::Progress::new);
    static NO_PRESERVE_SETTINGS: std::sync::LazyLock<preserve::Settings> =
        std::sync::LazyLock::new(preserve::preserve_none);
    static DO_PRESERVE_SETTINGS: std::sync::LazyLock<preserve::Settings> =
        std::sync::LazyLock::new(preserve::preserve_all);

    async fn setup_test_dirs(preserve: bool) -> Result<std::path::PathBuf> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        copy::copy(
            &PROGRESS,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &copy::Settings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                overwrite_filter: None,
                ignore_existing: false,
                chunk_size: 0,
                remote_copy_buffer_size: 0,
                filter: None,
                dry_run: None,
            },
            if preserve {
                &DO_PRESERVE_SETTINGS
            } else {
                &NO_PRESERVE_SETTINGS
            },
            false,
        )
        .await?;
        Ok(tmp_dir)
    }

    async fn truncate_file(path: &str) -> Result<()> {
        let file = tokio::fs::File::create(path).await?;
        file.set_len(0).await?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn check_basic_cmp() -> Result<()> {
        let tmp_dir = setup_test_dirs(true).await?;
        // drop 1 file from src
        tokio::fs::remove_file(&tmp_dir.join("foo").join("bar").join("1.txt")).await?;
        // sleep to ensure mtime is different, this acts as a poor-mans barrier
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        // modify 1 file in dst
        truncate_file(
            tmp_dir
                .join("bar")
                .join("baz")
                .join("4.txt")
                .to_str()
                .unwrap(),
        )
        .await?;
        // drop 1 (other) file from dst
        tokio::fs::remove_file(&tmp_dir.join("bar").join("bar").join("2.txt")).await?;
        // create one more file in dst -- this will also modify the mtime of the directory
        tokio::fs::File::create(&tmp_dir.join("bar").join("baz").join("7.txt")).await?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                ObjType::Dir => filecmp::MetadataCmpSettings {
                    mtime: true,
                    ..Default::default()
                },
                ObjType::Symlink => filecmp::MetadataCmpSettings {
                    mtime: true,
                    ..Default::default()
                },
                ObjType::Other => filecmp::MetadataCmpSettings {
                    mtime: true,
                    ..Default::default()
                },
            },
            filter: None,
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::new(
                Some(tmp_dir.join("cmp.log").as_path()),
                false,
                OutputFormat::Text,
            )
            .await?,
            &compare_settings,
        )
        .await?;
        let mismatch: Mismatch = enum_map! {
            ObjType::File => enum_map! {
                CompareResult::Different => 1,
                CompareResult::Same => 2,
                CompareResult::SrcMissing => 2,
                CompareResult::DstMissing => 1,
            },
            ObjType::Dir => enum_map! {
                CompareResult::Different => 2,
                CompareResult::Same => 1,
                CompareResult::SrcMissing => 0,
                CompareResult::DstMissing => 0,
            },
            ObjType::Symlink => enum_map! {
                CompareResult::Different => 0,
                CompareResult::Same => 2,
                CompareResult::SrcMissing => 0,
                CompareResult::DstMissing => 0,
            },
            ObjType::Other => enum_map! {
                CompareResult::Different => 0,
                CompareResult::Same => 0,
                CompareResult::SrcMissing => 0,
                CompareResult::DstMissing => 0,
            },
        };
        assert_eq!(summary.mismatch, mismatch);
        // src has 4 regular files of 1 byte each (0.txt, bar/2.txt, bar/3.txt, baz/4.txt)
        assert_eq!(summary.src_bytes, 4);
        // dst has: 0.txt(1B), bar/1.txt(1B, SrcMissing), bar/3.txt(1B), baz/4.txt(0B, truncated), baz/7.txt(0B, SrcMissing)
        assert_eq!(summary.dst_bytes, 3);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn cmp_with_filter_excludes_files() -> Result<()> {
        let tmp_dir = setup_test_dirs(true).await?;
        // setup: src=foo, dst=bar (identical at this point)
        // add a file to dst that would be reported as SrcMissing
        tokio::fs::write(&tmp_dir.join("bar").join("extra.txt"), "extra").await?;
        // without filter, should report extra.txt as SrcMissing
        let compare_settings_no_filter = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: None,
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::silent().await?,
            &compare_settings_no_filter,
        )
        .await?;
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::SrcMissing],
            1
        );
        // with filter excluding extra.txt, should not report it
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_exclude("extra.txt")?;
        let compare_settings_with_filter = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: Some(filter),
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::silent().await?,
            &compare_settings_with_filter,
        )
        .await?;
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::SrcMissing],
            0
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn cmp_with_include_only_compares_matching() -> Result<()> {
        let tmp_dir = setup_test_dirs(true).await?;
        // setup: src=foo, dst=bar (identical at this point)
        // modify a file that won't be included
        tokio::fs::write(&tmp_dir.join("bar").join("bar").join("1.txt"), "modified").await?;
        // with include pattern for only *.rs files, the .txt modification shouldn't appear
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_include("*.rs")?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: Some(filter),
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // no differences should be reported since all .txt files are excluded
        assert_eq!(summary.mismatch[ObjType::File][CompareResult::Different], 0);
        assert_eq!(summary.mismatch[ObjType::File][CompareResult::Same], 0);
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::SrcMissing],
            0
        );
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::DstMissing],
            0
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn cmp_with_path_pattern_filters_nested() -> Result<()> {
        // test that path-based patterns like "bar/*.txt" work correctly when recursing
        // this verifies source_root tracking is working properly
        let tmp_dir = setup_test_dirs(true).await?;
        // test structure:
        // foo/bar/1.txt, foo/bar/2.txt, foo/bar/3.txt
        // foo/baz/4.txt, foo/baz/5.txt (symlink), foo/baz/6.txt (symlink)
        // filter: only include bar/*.txt
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_include("bar/*.txt")?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings {
                    size: true,
                    ..Default::default()
                },
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: Some(filter),
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // should only compare files in bar/ subdirectory (3 files: 1.txt, 2.txt, 3.txt)
        // all should be "Same" since we copied foo to bar earlier
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::Same],
            3,
            "should have 3 same files from bar/*.txt pattern"
        );
        // files in baz/ should not be compared (filtered out)
        // 0.txt at root should not be compared
        assert_eq!(summary.mismatch[ObjType::File][CompareResult::Different], 0);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn cmp_filter_applies_to_root_file() -> Result<()> {
        // test that filters apply to the root item itself
        let tmp_dir = testutils::create_temp_dir().await?;
        // create two different files
        tokio::fs::write(tmp_dir.join("test.txt"), "content1").await?;
        tokio::fs::write(tmp_dir.join("test2.txt"), "content2").await?;
        // filter: only include *.rs files
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_include("*.rs")?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings {
                    size: true,
                    ..Default::default()
                },
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: Some(filter),
        };
        // compare test.txt vs test2.txt - should be filtered out (not *.rs)
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("test.txt"),
            &tmp_dir.join("test2.txt"),
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // should return empty summary since root file is filtered
        assert_eq!(summary.mismatch[ObjType::File][CompareResult::Same], 0);
        assert_eq!(summary.mismatch[ObjType::File][CompareResult::Different], 0);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn cmp_filter_excludes_root_directory() -> Result<()> {
        // test that filters apply to root directories
        let tmp_dir = testutils::setup_test_dir().await?;
        // filter: exclude directories named "foo"
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_exclude("foo")?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings::default(),
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: Some(filter),
        };
        // compare foo vs bar - foo should be filtered out
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // should return empty summary since root dir is excluded
        assert_eq!(summary.mismatch[ObjType::Dir][CompareResult::Same], 0);
        assert_eq!(summary.mismatch[ObjType::Dir][CompareResult::Different], 0);
        assert_eq!(summary.mismatch[ObjType::File][CompareResult::Same], 0);
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn cmp_combined_include_exclude_patterns() -> Result<()> {
        let tmp_dir = setup_test_dirs(true).await?;
        // include all .txt files, but exclude bar/2.txt specifically
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_include("**/*.txt")?;
        filter.add_exclude("bar/2.txt")?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings {
                    size: true,
                    ..Default::default()
                },
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: Some(filter),
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // should compare: 0.txt, bar/1.txt, bar/3.txt, baz/4.txt = 4 files (same)
        // should skip: bar/2.txt (excluded by pattern), 5.txt and 6.txt (symlinks, no match for *.txt in src dir) = 1 file + 2 symlinks
        // note: the pattern **/*.txt only matches files with .txt extension, but 5.txt and 6.txt in baz are symlinks
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::Same],
            4,
            "should compare 4 .txt files as same"
        );
        // bar/2.txt is skipped for both src and dst traversal = 2 skipped
        assert_eq!(
            summary.skipped[ObjType::File],
            2,
            "should skip 2 files (bar/2.txt on src and dst)"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn cmp_skipped_counts_comprehensive() -> Result<()> {
        let tmp_dir = setup_test_dirs(true).await?;
        // exclude bar/ directory entirely
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_exclude("bar/")?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings {
                    size: true,
                    ..Default::default()
                },
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: Some(filter),
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // compared: 0.txt (same), baz/4.txt (same) = 2 files
        // compared: baz/5.txt symlink (same), baz/6.txt symlink (same) = 2 symlinks
        // skipped: bar directory in src and dst = 2 dirs (cmp traverses both)
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::Same],
            2,
            "should compare 2 files as same"
        );
        assert_eq!(
            summary.mismatch[ObjType::Symlink][CompareResult::Same],
            2,
            "should compare 2 symlinks as same"
        );
        assert_eq!(
            summary.skipped[ObjType::Dir],
            2,
            "should skip 2 directories (bar in src + bar in dst)"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn expand_missing_dst_reports_all_entries() -> Result<()> {
        let tmp_dir = setup_test_dirs(true).await?;
        // remove bar/bar directory entirely from dst
        tokio::fs::remove_dir_all(&tmp_dir.join("bar").join("bar")).await?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: true,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings::default(),
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: None,
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // bar/bar dir has: bar/ (1 dir) + 1.txt, 2.txt, 3.txt (3 files)
        assert_eq!(
            summary.mismatch[ObjType::Dir][CompareResult::DstMissing],
            1,
            "should report 1 directory as DstMissing"
        );
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::DstMissing],
            3,
            "should report 3 files as DstMissing"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn expand_missing_src_reports_all_entries() -> Result<()> {
        let tmp_dir = setup_test_dirs(true).await?;
        // create a new subdir in dst with files
        let newdir = tmp_dir.join("bar").join("newdir");
        tokio::fs::create_dir(&newdir).await?;
        tokio::fs::write(newdir.join("a.txt"), "a").await?;
        tokio::fs::write(newdir.join("b.txt"), "b").await?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: true,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings::default(),
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: None,
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        assert_eq!(
            summary.mismatch[ObjType::Dir][CompareResult::SrcMissing],
            1,
            "should report 1 directory as SrcMissing"
        );
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::SrcMissing],
            2,
            "should report 2 files as SrcMissing"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn expand_missing_dst_deeply_nested() -> Result<()> {
        // verify expansion recurses through multiple directory levels
        let tmp_dir = testutils::create_temp_dir().await?;
        let src = tmp_dir.join("src");
        let dst = tmp_dir.join("dst");
        tokio::fs::create_dir(&src).await?;
        tokio::fs::create_dir(&dst).await?;
        // create src/a/b/c/d.txt -- 3 dirs deep
        let deep = src.join("a").join("b").join("c");
        tokio::fs::create_dir_all(&deep).await?;
        tokio::fs::write(deep.join("d.txt"), "d").await?;
        // also add a sibling file at an intermediate level
        tokio::fs::write(src.join("a").join("b").join("mid.txt"), "m").await?;
        // dst exists but is empty -- everything in src is DstMissing
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: true,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings::default(),
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: None,
        };
        let summary = cmp(
            &PROGRESS,
            &src,
            &dst,
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // dirs: a, a/b, a/b/c = 3 DstMissing dirs
        assert_eq!(
            summary.mismatch[ObjType::Dir][CompareResult::DstMissing],
            3,
            "should report 3 nested directories as DstMissing"
        );
        // files: a/b/c/d.txt, a/b/mid.txt = 2 DstMissing files
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::DstMissing],
            2,
            "should report 2 files as DstMissing"
        );
        // src_bytes: d.txt(1) + mid.txt(1) = 2
        assert_eq!(
            summary.src_bytes, 2,
            "should track bytes for expanded files"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn expand_missing_src_deeply_nested() -> Result<()> {
        // verify expansion recurses for SrcMissing through multiple levels
        let tmp_dir = testutils::create_temp_dir().await?;
        let src = tmp_dir.join("src");
        let dst = tmp_dir.join("dst");
        tokio::fs::create_dir(&src).await?;
        tokio::fs::create_dir(&dst).await?;
        // create dst/x/y/z.txt -- dirs only in dst
        let deep = dst.join("x").join("y");
        tokio::fs::create_dir_all(&deep).await?;
        tokio::fs::write(deep.join("z.txt"), "zz").await?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: true,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings::default(),
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: None,
        };
        let summary = cmp(
            &PROGRESS,
            &src,
            &dst,
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // dirs: x, x/y = 2 SrcMissing dirs
        assert_eq!(
            summary.mismatch[ObjType::Dir][CompareResult::SrcMissing],
            2,
            "should report 2 nested directories as SrcMissing"
        );
        // files: x/y/z.txt = 1 SrcMissing file
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::SrcMissing],
            1,
            "should report 1 file as SrcMissing"
        );
        // dst_bytes: z.txt(2)
        assert_eq!(
            summary.dst_bytes, 2,
            "should track bytes for expanded files"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn expand_missing_with_exclude_filter() -> Result<()> {
        // verify that filters are applied during expansion. exclude *.log files
        // from the missing subtree
        let tmp_dir = testutils::create_temp_dir().await?;
        let src = tmp_dir.join("src");
        let dst = tmp_dir.join("dst");
        tokio::fs::create_dir(&src).await?;
        tokio::fs::create_dir(&dst).await?;
        // src/missing_dir/ has mixed files
        let missing = src.join("missing_dir");
        tokio::fs::create_dir(&missing).await?;
        tokio::fs::write(missing.join("keep.txt"), "k").await?;
        tokio::fs::write(missing.join("skip.log"), "s").await?;
        tokio::fs::write(missing.join("also_keep.txt"), "a").await?;
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_exclude("*.log")?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: true,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings::default(),
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: Some(filter),
        };
        let summary = cmp(
            &PROGRESS,
            &src,
            &dst,
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // missing_dir itself = 1 DstMissing dir
        assert_eq!(summary.mismatch[ObjType::Dir][CompareResult::DstMissing], 1,);
        // only keep.txt and also_keep.txt should be reported. skip.log is filtered
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::DstMissing],
            2,
            "should report only non-excluded files as DstMissing"
        );
        // skip.log should be counted as skipped
        assert_eq!(
            summary.skipped[ObjType::File],
            1,
            "should count excluded file as skipped"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn expand_missing_with_include_filter() -> Result<()> {
        // verify that include filters restrict which children are reported during expansion
        let tmp_dir = testutils::create_temp_dir().await?;
        let src = tmp_dir.join("src");
        let dst = tmp_dir.join("dst");
        tokio::fs::create_dir(&src).await?;
        tokio::fs::create_dir(&dst).await?;
        // src/data/ has a mix of file types
        let data = src.join("data");
        tokio::fs::create_dir(&data).await?;
        tokio::fs::write(data.join("a.rs"), "fn main() {}").await?;
        tokio::fs::write(data.join("b.txt"), "hello").await?;
        tokio::fs::write(data.join("c.rs"), "fn test() {}").await?;
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_include("**/*.rs")?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: true,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings::default(),
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: Some(filter),
        };
        let summary = cmp(
            &PROGRESS,
            &src,
            &dst,
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // data dir = 1 DstMissing dir
        assert_eq!(summary.mismatch[ObjType::Dir][CompareResult::DstMissing], 1,);
        // only a.rs and c.rs should be reported. b.txt is filtered out
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::DstMissing],
            2,
            "should report only included files as DstMissing"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn expand_missing_with_nested_path_filter() -> Result<()> {
        // verify path-based patterns work correctly during expansion.
        // only include files under a specific nested path
        let tmp_dir = testutils::create_temp_dir().await?;
        let src = tmp_dir.join("src");
        let dst = tmp_dir.join("dst");
        tokio::fs::create_dir(&src).await?;
        tokio::fs::create_dir(&dst).await?;
        // src/top/ has two subdirs: keep/ and skip/
        let top = src.join("top");
        let keep = top.join("keep");
        let skip = top.join("skip");
        tokio::fs::create_dir_all(&keep).await?;
        tokio::fs::create_dir_all(&skip).await?;
        tokio::fs::write(keep.join("1.txt"), "1").await?;
        tokio::fs::write(keep.join("2.txt"), "2").await?;
        tokio::fs::write(skip.join("3.txt"), "3").await?;
        let mut filter = crate::filter::FilterSettings::new();
        filter.add_include("top/keep/**")?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: true,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings::default(),
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: Some(filter),
        };
        let summary = cmp(
            &PROGRESS,
            &src,
            &dst,
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // only keep/ subtree: keep dir(1) + top dir(1) = 2 dirs. skip dir is filtered
        assert_eq!(
            summary.mismatch[ObjType::Dir][CompareResult::DstMissing],
            2,
            "should report top and keep dirs as DstMissing"
        );
        // only 1.txt and 2.txt from keep/
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::DstMissing],
            2,
            "should report only files under keep/ as DstMissing"
        );
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn expand_missing_false_preserves_original_behavior() -> Result<()> {
        let tmp_dir = setup_test_dirs(true).await?;
        // remove bar/bar directory entirely from dst
        tokio::fs::remove_dir_all(&tmp_dir.join("bar").join("bar")).await?;
        let compare_settings = Settings {
            fail_early: false,
            exit_early: false,
            expand_missing: false,
            compare: enum_map! {
                ObjType::File => filecmp::MetadataCmpSettings::default(),
                ObjType::Dir => filecmp::MetadataCmpSettings::default(),
                ObjType::Symlink => filecmp::MetadataCmpSettings::default(),
                ObjType::Other => filecmp::MetadataCmpSettings::default(),
            },
            filter: None,
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::silent().await?,
            &compare_settings,
        )
        .await?;
        // without expand_missing, only the top-level dir is reported
        assert_eq!(
            summary.mismatch[ObjType::Dir][CompareResult::DstMissing],
            1,
            "should report only 1 directory as DstMissing"
        );
        assert_eq!(
            summary.mismatch[ObjType::File][CompareResult::DstMissing],
            0,
            "should not report individual files as DstMissing"
        );
        Ok(())
    }

    #[test]
    fn path_to_json_string_utf8() {
        let path = std::path::Path::new("/foo/bar/baz.txt");
        assert_eq!(path_to_json_string(path), "/foo/bar/baz.txt");
    }

    #[test]
    fn path_to_json_string_non_utf8() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        // embed 0xFF byte in the middle
        let os_str = OsStr::from_bytes(b"/tmp/bad\xffname.txt");
        let path = std::path::Path::new(os_str);
        assert_eq!(path_to_json_string(path), "/tmp/bad\\xffname.txt");
    }

    #[test]
    fn path_to_json_string_multiple_bad_bytes() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let os_str = OsStr::from_bytes(b"\x80/ok/\xfe\xff/end");
        let path = std::path::Path::new(os_str);
        assert_eq!(path_to_json_string(path), "\\x80/ok/\\xfe\\xff/end");
    }

    #[test]
    fn path_to_json_string_escapes_backslashes() {
        // a path with a literal backslash must be escaped so it doesn't
        // collide with \xHH byte escapes
        let path = std::path::Path::new("/tmp/bad\\xffname.txt");
        assert_eq!(path_to_json_string(path), "/tmp/bad\\\\xffname.txt");
    }

    #[test]
    fn path_to_json_string_no_collision() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        // literal backslash-x-f-f in the filename
        let literal = std::path::Path::new("/tmp/bad\\xffname.txt");
        // actual 0xFF byte in the filename
        let raw = std::path::Path::new(OsStr::from_bytes(b"/tmp/bad\xffname.txt"));
        // these must produce different output
        assert_ne!(path_to_json_string(literal), path_to_json_string(raw));
    }
}
