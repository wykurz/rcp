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
}

async fn async_main(args: Args) -> Result<common::LinkSummary> {
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
        std::path::PathBuf::from(args.dst)
    };
    let result = common::link(
        &args.src,
        &dst,
        &args.update,
        &common::LinkSettings {
            copy_settings: common::CopySettings {
                dereference: false, // currently not supported
                fail_early: args.fail_early,
                overwrite: args.overwrite,
                overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)?,
            },
            update_compare: common::parse_metadata_cmp_settings(&args.update_compare)?,
            update_exclusive: args.update_exclusive,
        },
    )
    .await;
    match result {
        Ok(summary) => Ok(summary),
        Err(error) => {
            event!(Level::ERROR, "{}", &error);
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
            Some(("link", args.progress_type.unwrap_or_default()))
        } else {
            None
        },
        args.quiet,
        args.verbose,
        args.summary,
        args.max_workers,
        args.max_blocking_threads,
        args.max_open_files,
        func,
    );
    match res {
        Ok(_) => std::process::exit(0),
        Err(_) => std::process::exit(1),
    }
}
