use anyhow::{anyhow, Context, Result};
use async_recursion::async_recursion;
use enum_map::{Enum, EnumMap};
use tokio::io::AsyncWriteExt;
use tracing::{event, instrument, Level};

use crate::copy::is_file_type_same;
use crate::filecmp;
use crate::progress;

#[derive(Copy, Clone, Debug, Enum)]
pub enum CmpResult {
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
}

pub type ObjCmpSettings = EnumMap<ObjType, filecmp::MetadataCmpSettings>;

#[derive(Debug, Copy, Clone)]
pub struct CmpSettings {
    pub compare: ObjCmpSettings,
    pub fail_early: bool,
    pub exit_early: bool,
}

pub type Mismatch = EnumMap<ObjType, EnumMap<CmpResult, u64>>;

#[derive(Default)]
pub struct CmpSummary {
    pub mismatch: Mismatch,
}

impl std::ops::Add for CmpSummary {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        let mut mismatch = self.mismatch;
        for (obj_type, &cmp_res_map) in &other.mismatch {
            for (cmp_res, &count) in &cmp_res_map {
                mismatch[obj_type][cmp_res] += count;
            }
        }
        Self { mismatch }
    }
}

impl std::fmt::Display for CmpSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for (obj_type, &cmp_res_map) in &self.mismatch {
            for (cmp_res, &count) in &cmp_res_map {
                writeln!(f, "{:?} {:?}: {}", obj_type, cmp_res, count)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LogWriter {
    log_opt: Option<std::sync::Arc<tokio::sync::Mutex<tokio::io::BufWriter<tokio::fs::File>>>>,
}

impl LogWriter {
    pub async fn new(log_path_opt: Option<&std::path::Path>) -> Result<Self> {
        if let Some(log_path) = log_path_opt {
            let log_file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(log_path)
                .await
                .with_context(|| format!("Failed to open log file: {:?}", log_path))?;
            let log =
                std::sync::Arc::new(tokio::sync::Mutex::new(tokio::io::BufWriter::new(log_file)));
            Ok(Self { log_opt: Some(log) })
        } else {
            Ok(Self { log_opt: None })
        }
    }

    pub async fn log_mismatch(
        &self,
        cmp_result: CmpResult,
        src_obj_type: Option<ObjType>,
        src: &std::path::Path,
        dst_obj_type: Option<ObjType>,
        dst: &std::path::Path,
    ) -> Result<()> {
        self.write(&format!(
            "[{:?}]\n\t[{:?}]\t{:?}\n\t[{:?}]\t{:?}\n",
            cmp_result, src_obj_type, src, dst_obj_type, dst
        ))
        .await
    }

    async fn write(&self, msg: &str) -> Result<()> {
        if let Some(log) = &self.log_opt {
            let mut log = log.lock().await;
            log.write_all(msg.as_bytes())
                .await
                .context("Failed to write to log file")?;
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        if let Some(log) = &self.log_opt {
            let mut log = log.lock().await;
            log.flush().await.context("Failed to flush log file")?;
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
        unreachable!("Unknown object type! {:?}", &metadata);
    }
}

#[instrument(skip(prog_track))]
#[async_recursion]
pub async fn cmp(
    prog_track: &'static progress::Progress,
    src: &std::path::Path,
    dst: &std::path::Path,
    log: &LogWriter,
    settings: &CmpSettings,
) -> Result<CmpSummary> {
    throttle::get_ops_token().await;
    let _prog_guard = prog_track.ops.guard();
    event!(Level::DEBUG, "reading source metadata");
    // it is impossible for src not exist other than user passing invalid path (which is an error)
    let src_metadata = tokio::fs::symlink_metadata(src)
        .await
        .with_context(|| format!("failed reading metadata from {:?}", &src))?;
    let mut cmp_summary = CmpSummary::default();
    let src_obj_type = obj_type(&src_metadata);
    let dst_metadata = {
        match tokio::fs::symlink_metadata(dst).await {
            Ok(metadata) => metadata,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    cmp_summary.mismatch[src_obj_type][CmpResult::DstMissing] += 1;
                    log.log_mismatch(CmpResult::DstMissing, Some(src_obj_type), src, None, dst)
                        .await?;
                    return Ok(cmp_summary);
                }
                return Err(err).context(format!("failed reading metadata from {:?}", &dst));
            }
        }
    };
    if !is_file_type_same(&src_metadata, &dst_metadata)
        || !filecmp::metadata_equal(
            &settings.compare[src_obj_type],
            &src_metadata,
            &dst_metadata,
        )
    {
        // we use the src type for the summary attribution
        cmp_summary.mismatch[src_obj_type][CmpResult::Different] += 1;
        let dst_obj_type = obj_type(&dst_metadata);
        log.log_mismatch(
            CmpResult::Different,
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
        cmp_summary.mismatch[src_obj_type][CmpResult::Same] += 1;
    }
    if !src_metadata.is_dir() || !dst_metadata.is_dir() {
        // nothing more to do
        return Ok(cmp_summary);
    }
    event!(Level::DEBUG, "process contents of 'src' directory");
    let mut src_entries = tokio::fs::read_dir(src)
        .await
        .with_context(|| format!("cannot open directory {:?} for reading", src))?;
    let mut join_set = tokio::task::JoinSet::new();
    let mut success = true;
    // create a set of all the files we already processed
    let mut processed_files = std::collections::HashSet::new();
    // iterate through src entries and recursively call "cmp" on each one
    while let Some(src_entry) = src_entries
        .next_entry()
        .await
        .with_context(|| format!("failed traversing directory {:?}", &src))?
    {
        let entry_path = src_entry.path();
        let entry_name = entry_path.file_name().unwrap();
        processed_files.insert(entry_name.to_owned());
        let dst_path = dst.join(entry_name);
        let log = log.clone();
        let settings = *settings;
        let do_cmp =
            || async move { cmp(prog_track, &entry_path, &dst_path, &log, &settings).await };
        join_set.spawn(do_cmp());
    }
    // unfortunately ReadDir is opening file-descriptors and there's not a good way to limit this,
    // one thing we CAN do however is to drop it as soon as we're done with it
    drop(src_entries);
    event!(Level::DEBUG, "process contents of 'dst' directory");
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
        event!(Level::DEBUG, "found a new entry in the 'dst' directory");
        let dst_path = dst.join(entry_name);
        let dst_entry_metadata = tokio::fs::symlink_metadata(&dst_path)
            .await
            .with_context(|| format!("failed reading metadata from {:?}", &dst_path))?;
        let dst_obj_type = obj_type(&dst_entry_metadata);
        cmp_summary.mismatch[dst_obj_type][CmpResult::SrcMissing] += 1;
        log.log_mismatch(
            CmpResult::SrcMissing,
            None,
            &src.join(entry_name),
            Some(dst_obj_type),
            &dst_path,
        )
        .await?;
    }
    // unfortunately ReadDir is opening file-descriptors and there's not a good way to limit this,
    // one thing we CAN do however is to drop it as soon as we're done with it
    drop(dst_entries);
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(summary) => cmp_summary = cmp_summary + summary,
            Err(error) => {
                event!(
                    Level::ERROR,
                    "cmp: {:?} vs {:?} failed with: {}",
                    src,
                    dst,
                    &error
                );
                if settings.fail_early {
                    return Err(error);
                }
                success = false;
            }
        }
    }
    if !success {
        return Err(anyhow!("cmp: {:?} vs {:?} failed!", src, dst));
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

    lazy_static! {
        static ref PROGRESS: progress::Progress = progress::Progress::new();
        static ref NO_PRESERVE_SETTINGS: preserve::PreserveSettings = preserve::preserve_default();
        static ref DO_PRESERVE_SETTINGS: preserve::PreserveSettings = preserve::preserve_all();
    }

    async fn setup_test_dirs(preserve: bool) -> Result<std::path::PathBuf> {
        let tmp_dir = testutils::setup_test_dir().await?;
        let test_path = tmp_dir.as_path();
        copy::copy(
            &PROGRESS,
            &test_path,
            &test_path.join("foo"),
            &test_path.join("bar"),
            &copy::CopySettings {
                dereference: false,
                fail_early: false,
                overwrite: false,
                overwrite_compare: filecmp::MetadataCmpSettings {
                    size: true,
                    mtime: true,
                    ..Default::default()
                },
                chunk_size: 0,
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
            &tmp_dir
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
        let compare_settings = CmpSettings {
            fail_early: false,
            exit_early: false,
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
            },
        };
        let summary = cmp(
            &PROGRESS,
            &tmp_dir.join("foo"),
            &tmp_dir.join("bar"),
            &LogWriter::new(Some(&tmp_dir.join("cmp.log").as_path())).await?,
            &compare_settings,
        )
        .await?;
        let mismatch: Mismatch = enum_map! {
            ObjType::File => enum_map! {
                CmpResult::Different => 1,
                CmpResult::Same => 2,
                CmpResult::SrcMissing => 2,
                CmpResult::DstMissing => 1,
            },
            ObjType::Dir => enum_map! {
                CmpResult::Different => 2,
                CmpResult::Same => 1,
                CmpResult::SrcMissing => 0,
                CmpResult::DstMissing => 0,
            },
            ObjType::Symlink => enum_map! {
                CmpResult::Different => 0,
                CmpResult::Same => 2,
                CmpResult::SrcMissing => 0,
                CmpResult::DstMissing => 0,
            },
        };
        assert_eq!(summary.mismatch, mismatch);
        Ok(())
    }
}
