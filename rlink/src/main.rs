use anyhow::{anyhow, Context, Result};
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "rlink",
    version,
    about = "`rlink` allows hard-linking large number of files.

A common pattern is to also provide `--update <path>` that overrides any paths in `src` to instead be copied over from there."
)]
struct Args {
    // Linking options
    /// Overwrite existing files/directories
    #[arg(short, long, help_heading = "Linking options")]
    overwrite: bool,

    /// Comma separated list of file attributes to compare when deciding if files are "identical", used with --overwrite flag
    ///
    /// Options are: uid, gid, mode, size, mtime, ctime
    #[arg(
        long,
        default_value = "size,mtime",
        value_name = "OPTIONS",
        help_heading = "Linking options"
    )]
    overwrite_compare: String,

    /// Exit on first error
    #[arg(short = 'e', long = "fail-early", help_heading = "Linking options")]
    fail_early: bool,

    /// Directory with updated contents of `link`
    #[arg(long, value_name = "PATH", help_heading = "Linking options")]
    update: Option<std::path::PathBuf>,

    /// Hard-link only the files that are in the update directory
    #[arg(long, help_heading = "Linking options")]
    update_exclusive: bool,

    /// Same as overwrite-compare, but for deciding if we can hard-link or if we need to copy a file from the update directory
    ///
    /// Used with --update flag
    #[arg(
        long,
        default_value = "size,mtime",
        value_name = "OPTIONS",
        help_heading = "Linking options"
    )]
    update_compare: String,

    // Progress & output
    /// Show progress
    #[arg(long, help_heading = "Progress & output")]
    progress: bool,

    /// Toggles the type of progress to show
    ///
    /// If specified, --progress flag is implied.
    ///
    /// Options are: ProgressBar (animated progress bar), TextUpdates (appropriate for logging), Auto (default, will
    /// choose between ProgressBar or TextUpdates depending on the type of terminal attached to stderr)
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

    /// Print summary at the end
    #[arg(long, help_heading = "Progress & output")]
    summary: bool,

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count, help_heading = "Progress & output")]
    verbose: u8,

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
    /// Directory with contents we want to update into `dst`
    #[arg()]
    src: std::path::PathBuf,

    /// Directory where we put either a hard-link of a file from `link` if it was unchanged, or a copy of a file from `new` if it's been modified
    #[arg()]
    dst: String, // must be a string to allow for parsing trailing slash
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
            tracing::error!("{:?}", &error);
            if args.summary {
                return Err(anyhow!("rlink encountered errors\n\n{}", &error.summary));
            }
            Err(anyhow!("rlink encountered errors"))
        }
    }
}

fn main() -> Result<()> {
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
