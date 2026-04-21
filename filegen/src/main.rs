use anyhow::Result;
use clap::Parser;
use tracing::instrument;

#[derive(Clone, Debug)]
struct Dirwidth {
    value: Vec<usize>,
}

impl std::str::FromStr for Dirwidth {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        if s.is_empty() {
            anyhow::bail!(
                "Invalid dirwidth specification: must contain at least one value (e.g., \"3,2\")"
            );
        }
        let value = s
            .split(',')
            .map(str::parse::<usize>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| anyhow::anyhow!("Invalid dirwidth specification '{}': {}", s, e))?;
        // validate that all values are > 0
        if let Some((index, _)) = value.iter().enumerate().find(|&(_, &v)| v == 0) {
            anyhow::bail!(
                "Invalid dirwidth specification '{}': value at position {} is 0. All values must be greater than 0.",
                s,
                index + 1
            );
        }
        Ok(Dirwidth { value })
    }
}

#[derive(Clone, Parser, Debug)]
#[command(
    name = "filegen",
    version,
    about = "Generate sample filesets for testing",
    long_about = "`filegen` generates sample filesets with configurable directory structure and file sizes.

EXAMPLE:
    # Generate a test fileset with 2 levels, 10 files per dir, 1MB each
    filegen /tmp 3,2 10 1M --progress

This creates a directory tree at /tmp/filegen/ with 3 top-level dirs, each containing 2 subdirs, with 10 files of 1MB each in every directory."
)]
struct Args {
    // ARGUMENTS
    /// Root directory where files are generated
    #[arg()]
    root: std::path::PathBuf,

    // Generation options
    /// Directory structure specification (comma-separated list of subdirectory counts per level)
    ///
    /// For example, "3,2" creates 3 top-level directories, each containing 2 subdirectories (total: 3 + 3×2 = 9 directories)
    #[arg(value_name = "SPEC", help_heading = "Generation options")]
    dirwidth: Dirwidth,

    /// Number of files in each directory
    #[arg(value_name = "N", help_heading = "Generation options")]
    numfiles: usize,

    /// Size of each file
    ///
    /// Accepts suffixes like "1K", "1M", "1G"
    #[arg(value_name = "SIZE", help_heading = "Generation options")]
    filesize: String,

    /// Size of the buffer used to write to each file
    ///
    /// Accepts suffixes like "1K", "1M", "1G"
    #[arg(
        long,
        default_value = "4K",
        value_name = "SIZE",
        help_heading = "Generation options"
    )]
    bufsize: String,

    /// Generate files only in leaf directories (deepest level), not in intermediate directories
    #[arg(long, help_heading = "Generation options")]
    leaf_files: bool,

    /// Print summary at the end
    #[arg(long, help_heading = "Progress & output")]
    summary: bool,

    /// Quiet mode, don't report errors
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    quiet: bool,

    /// Maximum number of open files (concurrent file writes)
    ///
    /// Since filegen's random data generation is CPU-intensive, the default is set to the number
    /// of physical CPU cores. This optimizes performance by matching concurrency to compute
    /// capacity rather than allowing excessive parallelism that would cause CPU contention.
    ///
    /// Set to 0 for no limit. Increase if using slow storage where I/O latency dominates.
    #[arg(long, value_name = "N", help_heading = "Performance & throttling")]
    max_open_files: Option<usize>,

    /// Chunk size used to calculate number of I/O per file
    ///
    /// Modifying this setting to a value > 0 is REQUIRED when using --iops-throttle.
    #[arg(
        long,
        default_value = "0",
        value_name = "SIZE",
        help_heading = "Performance & throttling"
    )]
    chunk_size: u64,

    #[command(flatten)]
    common: common::cli::CommonArgs,
}

#[instrument]
async fn async_main(args: Args) -> Result<common::filegen::Summary> {
    use anyhow::Context;
    let filesize = args
        .filesize
        .parse::<bytesize::ByteSize>()
        .unwrap()
        .as_u64() as usize;
    let writebuf = args.bufsize.parse::<bytesize::ByteSize>().unwrap().as_u64() as usize;
    let root = args.root.join("filegen");
    tokio::fs::create_dir(&root)
        .await
        .with_context(|| format!("Error creating {:?}", &root))
        .map_err(|err| common::filegen::Error::new(err, common::filegen::Summary::default()))?;
    let prog_track = common::get_progress();
    prog_track.directories_created.inc();
    let mut summary = common::filegen::Summary {
        directories_created: 1,
        ..Default::default()
    };
    let config = common::filegen::FileGenConfig {
        root: root.clone(),
        dirwidth: args.dirwidth.value.clone(),
        numfiles: args.numfiles,
        filesize,
        writebuf,
        chunk_size: args.chunk_size,
        leaf_files: args.leaf_files,
    };
    let filegen_summary = common::filegen::filegen(prog_track, &config).await?;
    summary = summary + filegen_summary;
    Ok(summary)
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    let output = args.common.output_config(args.quiet, args.summary);
    let runtime = args.common.runtime_config();
    // filegen's random data generation is CPU-intensive, so we default to
    // available parallelism rather than 80% of RLIMIT_NOFILE used by other tools.
    // use 1 as absolute minimum to avoid accidentally disabling limits.
    let max_open_files = args.max_open_files.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    });
    let throttle = args
        .common
        .throttle_config(Some(max_open_files), args.chunk_size);
    let tracing = common::TracingConfig {
        remote_layer: None,
        debug_log_file: None,
        chrome_trace_prefix: None,
        flamegraph_prefix: None,
        trace_identifier: "filegen".to_string(),
        profile_level: None,
        tokio_console: false,
        tokio_console_port: None,
    };
    // note: filegen historically does not treat --progress-delay alone as
    // implying --progress (unlike rrm/rlink). preserve that behavior here.
    let progress = if args.common.progress || args.common.progress_type.is_some() {
        Some(common::ProgressSettings {
            progress_type: common::GeneralProgressType::User(
                args.common.progress_type.unwrap_or_default(),
            ),
            progress_delay: args.common.progress_delay.clone(),
        })
    } else {
        None
    };
    let res = common::run(progress, output, runtime, throttle, tracing, func);
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}
