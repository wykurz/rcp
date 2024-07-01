use anyhow::{anyhow, Context, Result};
use common::ProgressType;
use structopt::StructOpt;
use tracing::{event, instrument, Level};

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "rcp",
    about = "`rcp` is a tool for copying files similar to `cp` but generally MUCH faster when dealing with a large number of files.

Inspired by tools like `dsync`(1) and `pcp`(2).

1) https://mpifileutils.readthedocs.io/en/v0.11.1/dsync.1.html
2) https://github.com/wtsi-ssg/pcp"
)]
struct Args {
    /// Overwrite existing files/directories
    #[structopt(short, long)]
    overwrite: bool,

    /// Comma separated list of file attributes to compare when when deciding if files are "identical", used with --overwrite flag.
    /// Options are: uid, gid, mode, size, mtime, ctime
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

    /// Preserve additional file attributes: file owner, group, setuid, setgid, mtime and atime
    #[structopt(short, long)]
    preserve: bool,

    /// Specify exactly what attributes to preserve.
    ///
    /// If specified, the "preserve" flag is ignored.
    ///
    /// The format is: "<type1>:<attributes1> <type2>:<attributes2> ..."
    /// Where <type> is one of: f (file), d (directory), l (symlink)
    /// And <attributes> is a comma separated list of: uid, gid, time, <mode mask>
    /// Where <mode mask> is a 4 digit octal number
    ///
    /// Example: "f:uid,gid,time,0777 d:uid,gid,time,0777 l:uid,gid,time"
    #[structopt(long)]
    preserve_settings: Option<String>,

    /// Always follow symbolic links in source
    #[structopt(short = "-L", long)]
    dereference: bool,

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,

    /// Print summary at the end
    #[structopt(long)]
    summary: bool,

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Source path(s) and destination path
    #[structopt()]
    paths: Vec<String>,

    /// Number of worker threads, 0 means number of cores
    #[structopt(long, default_value = "0")]
    max_workers: usize,

    /// Number of blocking worker threads, 0 means Tokio runtime default (512)
    #[structopt(long, default_value = "0")]
    max_blocking_threads: usize,
}

#[instrument]
async fn async_main(args: Args) -> Result<common::CopySummary> {
    if args.paths.len() < 2 {
        return Err(anyhow!(
            "You must specify at least one source and destination path!"
        ));
    }
    let src_strings = &args.paths[0..args.paths.len() - 1];
    for src in src_strings {
        if src == "." || src.ends_with("/.") {
            return Err(anyhow!(
                "expanding source directory ({:?}) using dot operator ('.') is not supported, please use absolute path or '*' instead",
                std::path::PathBuf::from(src))
            );
        }
    }
    let dst_string = args.paths.last().unwrap();
    let src_dst: Vec<(std::path::PathBuf, std::path::PathBuf)> = if dst_string.ends_with('/') {
        // rcp foo bar baz/ -> copy foo to baz/foo and bar to baz/bar
        let dst_dir = std::path::PathBuf::from(dst_string);
        src_strings
            .iter()
            .map(|src| {
                let src_path = std::path::PathBuf::from(src);
                let src_file = src_path
                    .file_name()
                    .context(format!("source {:?} does not have a basename", &src_path))
                    .unwrap();
                (src_path.to_owned(), dst_dir.join(src_file))
            })
            .collect::<Vec<(std::path::PathBuf, std::path::PathBuf)>>()
    } else {
        if src_strings.len() > 1 {
            return Err(anyhow!(
                "Multiple sources can only be copied to a directory; if this is your intent follow the destination path with a trailing slash"
            ));
        }
        assert_eq!(src_strings.len(), 1);
        vec![(
            std::path::PathBuf::from(src_strings[0].clone()),
            std::path::PathBuf::from(dst_string),
        )]
    };
    let mut join_set = tokio::task::JoinSet::new();
    let settings = common::CopySettings {
        dereference: args.dereference,
        fail_early: args.fail_early,
        overwrite: args.overwrite,
        overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)
            .map_err(|err| common::CopyError::new(err, Default::default()))?,
    };
    event!(Level::DEBUG, "copy settings: {:?}", &settings);
    if args.preserve_settings.is_some() && args.preserve {
        event!(
            Level::WARN,
            "The --preserve flag is ignored when --preserve-settings is specified!"
        );
    }
    let preserve = if let Some(preserve_settings) = args.preserve_settings {
        common::parse_preserve_settings(&preserve_settings)
            .map_err(|err| common::CopyError::new(err, Default::default()))?
    } else if args.preserve {
        common::preserve_all()
    } else {
        common::preserve_default()
    };
    event!(Level::DEBUG, "preserve settings: {:?}", &preserve);
    for (src_path, dst_path) in src_dst {
        let do_copy =
            || async move { common::copy(&src_path, &dst_path, &settings, &preserve).await };
        join_set.spawn(do_copy());
    }
    let mut success = true;
    let mut copy_summary = common::CopySummary::default();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(result) => match result {
                Ok(summary) => copy_summary = copy_summary + summary,
                Err(error) => {
                    event!(Level::ERROR, "{}", &error);
                    copy_summary = copy_summary + error.summary;
                    if args.fail_early {
                        if args.summary {
                            return Err(anyhow!("{}\n\n{}", error, &copy_summary));
                        }
                        return Err(anyhow!("{}", error));
                    }
                    success = false;
                }
            },
            Err(error) => {
                if settings.fail_early {
                    if args.summary {
                        return Err(anyhow!("{}\n\n{}", error, &copy_summary));
                    }
                    return Err(anyhow!("{}", error));
                }
            }
        }
    }
    if !success {
        if args.summary {
            return Err(anyhow!("rcp encountered errors\n\n{}", &copy_summary));
        }
        return Err(anyhow!("rcp encountered errors"));
    }
    Ok(copy_summary)
}

fn main() -> Result<(), anyhow::Error> {
    let args = Args::from_args();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    let res = common::run(
        if args.progress || args.progress_type.is_some() {
            Some(("copy", args.progress_type.unwrap_or_default()))
        } else {
            None
        },
        args.quiet,
        args.verbose,
        args.summary,
        args.max_workers,
        args.max_blocking_threads,
        func,
    );
    match res {
        Ok(_) => std::process::exit(0),
        Err(_) => std::process::exit(1),
    }
}
