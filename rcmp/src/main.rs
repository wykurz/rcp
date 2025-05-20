use anyhow::Result;
use common::ProgressType;
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "rcmp",
    about = "`rcmp` is a tool for comparing large filesets.

Currently, it only supports comparing metadata (no content checking).

Returns error code 1 if there are differences, 2 if there were errors."
)]
struct Args {
    /// Attributes to compare when when deciding if objects are "identical". Options are: uid, gid, mode, size, mtime, ctime
    ///
    /// The format is: "<type1>:<attributes1> <type2>:<attributes2> ..."
    /// Where <type> is one of: "f" (file), "d" (directory), "l" (symlink)
    /// And <attributes> is a comma separated list of: uid, gid, size, mtime, ctime
    ///
    /// Example: "f:mtime,ctime,mode,size d:mtime,ctime,mode l:mtime,ctime,mode"
    #[structopt(long, default_value = "f:mtime,size d:mtime l:mtime")]
    metadata_compare: String,

    /// Exit on first error
    #[structopt(short = "-e", long = "fail-early")]
    fail_early: bool,

    /// Exit on first mismatch
    #[structopt(short = "-m", long = "exit-early")]
    exit_early: bool,

    /// Show progress
    #[structopt(long)]
    progress: bool,

    /// Toggles the type of progress to show.
    ///
    /// If specified, --progress flag is implied.
    ///
    /// Options are: ProgressBar (animated progress bar), TextUpdates (appropriate for logging), Auto (default, will
    /// choose between ProgressBar or TextUpdates depending on the type of terminal attached to stderr)
    #[structopt(long)]
    progress_type: Option<ProgressType>,

    /// Sets the delay between progress updates.
    ///
    /// - For the interactive (--progress-type=ProgressBar), the default is 200ms.
    /// - For the non-interactive (--progress-type=TextUpdates), the default is 10s.
    ///
    /// If specified, --progress flag is implied.
    ///
    /// This option accepts a human readable duration, e.g. "200ms", "10s", "5min" etc.
    #[structopt(long)]
    progress_delay: Option<String>,

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,

    /// Return non-zero exit code only if there were errors performing the comparison.
    #[structopt(long)]
    no_check: bool,

    /// Print summary at the end
    #[structopt(long)]
    summary: bool,

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// File name where to store comparison mismatch output
    #[structopt(long)]
    log: Option<std::path::PathBuf>,

    /// File or directory to compare
    #[structopt()]
    src: std::path::PathBuf,

    /// File or directory to compare
    #[structopt()]
    dst: std::path::PathBuf,

    /// Number of worker threads, 0 means number of cores
    #[structopt(long, default_value = "0")]
    max_workers: usize,

    /// Number of blocking worker threads, 0 means Tokio runtime default (512)
    #[structopt(long, default_value = "0")]
    max_blocking_threads: usize,

    /// Maximum number of open files, 0 means no limit, leaving unspecified means using 80% of max open files system limit
    #[structopt(long)]
    max_open_files: Option<usize>,

    /// Throttle the number of operations per second, 0 means no throttle
    #[structopt(long, default_value = "0")]
    ops_throttle: usize,

    /// Throttle the number of I/O operations per second, 0 means no throttle.
    ///
    /// I/O is calculated based on provided chunk size -- number of I/O operations for a file is calculated as:
    /// ((file size - 1) / chunk size) + 1
    #[structopt(long, default_value = "0")]
    iops_throttle: usize,

    /// Chunk size used to calculate number of I/O per file.
    ///
    /// Modifying this setting to a value > 0 is REQUIRED when using --iops-throttle.
    #[structopt(long, default_value = "0")]
    chunk_size: u64,

    /// Throttle the number of bytes per second, 0 means no throttle
    #[structopt(long, default_value = "0")]
    tput_throttle: usize,
}

async fn async_main(args: Args) -> Result<common::CmpSummary> {
    let log_handle = common::LogWriter::new(args.log.as_deref()).await?;
    let summary = common::cmp(
        &args.src,
        &args.dst,
        &log_handle,
        &common::CmpSettings {
            fail_early: args.fail_early,
            exit_early: args.exit_early,
            compare: common::parse_compare_settings(&args.metadata_compare)?,
        },
    )
    .await?;
    log_handle.flush().await?;
    Ok(summary)
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    let res = common::run(
        if args.progress || args.progress_type.is_some() {
            Some(common::ProgressSettings {
                progress_type: args.progress_type.unwrap_or_default(),
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
        args.tput_throttle,
        func,
    );
    match res {
        Some(summary) => match args.no_check {
            // when --no-check is specified, return error code only if there were errors
            true => std::process::exit(0),
            false => {
                // if there are any differences, return error code 1
                for (_, cmp_result) in &summary.mismatch {
                    let different = cmp_result[common::CmpResult::Different] > 0
                        || cmp_result[common::CmpResult::SrcMissing] > 0
                        || cmp_result[common::CmpResult::DstMissing] > 0;
                    if different {
                        std::process::exit(1);
                    }
                }
                std::process::exit(0);
            }
        },
        None => std::process::exit(2),
    }
}
