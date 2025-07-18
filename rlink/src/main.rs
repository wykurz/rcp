use anyhow::{anyhow, Context, Result};
use common::ProgressType;
use structopt::StructOpt;
use tracing::{event, Level};

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "rlink",
    about = "`rlink` allows hard-linking large number of files.

A common pattern is to also provide `--update <path>` that overrides any paths in `src` to instead be copied over from there."
)]
struct Args {
    /// Overwrite existing files/directories
    #[structopt(short, long)]
    overwrite: bool,

    /// Comma separated list of file attributes to compare when when deciding if files are "identical", used with --overwrite flag. Options are: uid, gid, mode, size, mtime, ctime
    #[structopt(long, default_value = "size,mtime")]
    overwrite_compare: String,

    /// Exit on first error
    #[structopt(short = "-e", long = "fail-early")]
    fail_early: bool,

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

    /// Print summary at the end
    #[structopt(long)]
    summary: bool,

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Directory with contents we want to update into `dst`
    #[structopt()]
    src: std::path::PathBuf,

    /// Directory where we put either a hard-link of a file from `link` if it was unchanged, or a copy of a file from `new` if it's been modified
    #[structopt()]
    dst: String, // must be a string to allow for parsing trailing slash

    /// Directory with updated contents of `link`
    #[structopt(long)]
    update: Option<std::path::PathBuf>,

    /// Hard-link only the files that are in the update directory
    #[structopt(long)]
    update_exclusive: bool,

    /// Same as overwrite-compare, but for deciding if we can hard-link or if we need to copy a file from the update directory. Used with --update flag
    #[structopt(long, default_value = "size,mtime")]
    update_compare: String,

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

async fn async_main(args: Args) -> Result<common::link::Summary> {
    for src in &args.src {
        if src == "."
            || src
                .to_str()
                .expect("input path cannot be converted to string?!")
                .ends_with("/.")
        {
            return Err(anyhow!(
                "expanding source directory ({:?}) using dot operator ('.') is not supported, please use absolute path or '*' instead",
                std::path::PathBuf::from(src)
            ));
        }
    }
    let dst = if args.dst.ends_with('/') {
        let src_file = args
            .src
            .file_name()
            .context(format!("source {:?} does not have a basename", &args.src))
            .unwrap();
        let dst_dir = std::path::PathBuf::from(args.dst);
        dst_dir.join(src_file)
    } else {
        let dst_path = std::path::PathBuf::from(args.dst);
        if dst_path.exists() && !args.overwrite {
            return Err(anyhow!(
                "Destination path {dst_path:?} already exists! \n\
                If you want to copy INTO it then follow the destination path with a trailing slash (/) or use \
                --overwrite if you want to overwrite it"
            ));
        }
        dst_path
    };
    let result = common::link(
        &args.src,
        &dst,
        &args.update,
        &common::link::Settings {
            copy_settings: common::copy::Settings {
                dereference: false, // currently not supported
                fail_early: args.fail_early,
                overwrite: args.overwrite,
                overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)?,
                chunk_size: args.chunk_size,
            },
            update_compare: common::parse_metadata_cmp_settings(&args.update_compare)?,
            update_exclusive: args.update_exclusive,
        },
    )
    .await;
    match result {
        Ok(summary) => Ok(summary),
        Err(error) => {
            event!(Level::ERROR, "{:?}", &error);
            if args.summary {
                return Err(anyhow!("rlink encountered errors\n\n{}", &error.summary));
            }
            Err(anyhow!("rlink encountered errors"))
        }
    }
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
    if res.is_none() {
        std::process::exit(1);
    }
    Ok(())
}
