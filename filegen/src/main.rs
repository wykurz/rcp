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
        if let Some((index, _)) = value.iter().enumerate().find(|(_, &v)| v == 0) {
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

    // Progress & output
    /// Show progress
    #[arg(long, help_heading = "Progress & output")]
    progress: bool,

    /// Toggles the type of progress to show
    ///
    /// If specified, --progress flag is implied.
    ///
    /// Options are: `ProgressBar` (animated progress bar), `TextUpdates` (appropriate for logging), Auto (default, will
    /// choose between `ProgressBar` or `TextUpdates` depending on the type of terminal attached to stderr)
    #[arg(long, value_name = "TYPE", help_heading = "Progress & output")]
    progress_type: Option<common::ProgressType>,

    /// Sets the delay between progress updates
    ///
    /// - For the interactive (--progress-type=ProgressBar), the default is 200ms.
    /// - For the non-interactive (--progress-type=TextUpdates), the default is 10s.
    ///
    /// If specified, --progress flag is implied.
    ///
    /// This option accepts a human readable duration, e.g. "200ms", "10s", "5min" etc.
    #[arg(long, value_name = "DELAY", help_heading = "Progress & output")]
    progress_delay: Option<String>,

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count, help_heading = "Progress & output")]
    verbose: u8,

    /// Print summary at the end
    #[arg(long, help_heading = "Progress & output")]
    summary: bool,

    /// Quiet mode, don't report errors
    #[arg(short = 'q', long = "quiet", help_heading = "Progress & output")]
    quiet: bool,

    // Performance & throttling
    /// Maximum number of open files, 0 means no limit, leaving unspecified means using 80% of max open files system limit
    #[arg(long, value_name = "N", help_heading = "Performance & throttling")]
    max_open_files: Option<usize>,

    /// Throttle the number of operations per second, 0 means no throttle
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Performance & throttling"
    )]
    ops_throttle: usize,

    /// Throttle the number of I/O operations per second, 0 means no throttle
    ///
    /// I/O is calculated based on provided chunk size -- number of I/O operations for a file is calculated as:
    /// ((file size - 1) / chunk size) + 1
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Performance & throttling"
    )]
    iops_throttle: usize,

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

    // Advanced settings
    /// Number of worker threads, 0 means number of cores
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Advanced settings"
    )]
    max_workers: usize,

    /// Number of blocking worker threads, 0 means Tokio runtime default (512)
    #[arg(
        long,
        default_value = "0",
        value_name = "N",
        help_heading = "Advanced settings"
    )]
    max_blocking_threads: usize,

    // ARGUMENTS
    /// Root directory where files are generated
    #[arg()]
    root: std::path::PathBuf,
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
        .map_err(|err| {
            common::filegen::Error::new(
                anyhow::Error::msg(err),
                common::filegen::Summary::default(),
            )
        })?;
    let prog_track = common::get_progress();
    prog_track.directories_created.inc();
    let mut summary = common::filegen::Summary {
        directories_created: 1,
        ..Default::default()
    };
    let filegen_summary = common::filegen::filegen(
        prog_track,
        &root,
        &args.dirwidth.value,
        args.numfiles,
        filesize,
        writebuf,
        args.chunk_size,
        args.leaf_files,
    )
    .await?;
    summary = summary + filegen_summary;
    Ok(summary)
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    let res = common::run(
        if args.progress || args.progress_type.is_some() {
            Some(common::ProgressSettings {
                progress_type: common::GeneralProgressType::User(
                    args.progress_type.unwrap_or_default(),
                ),
                progress_delay: args.progress_delay,
            })
        } else {
            None
        },
        args.quiet,
        args.verbose,
        args.summary,
        args.max_workers,
        args.max_blocking_threads,
        args.max_open_files,
        args.ops_throttle,
        args.iops_throttle,
        args.chunk_size,
        None,
        None,
        func,
    );
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}
