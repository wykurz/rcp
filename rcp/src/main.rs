use anyhow::{Context, Result};
use common::CopySummary;
use structopt::StructOpt;
use tracing::{event, instrument, Level};

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "rcp")]
struct Args {
    /// Overwrite existing files/directories
    #[structopt(short, long)]
    overwrite: bool,

    /// Comma separated list of file attributes to compare when when deciding if files are "identical", used with --overwrite flag.
    /// Options are: uid, gid, size, mtime, ctime
    #[structopt(long, default_value = "size,mtime")]
    overwrite_compare: String,

    /// Exit on first error
    #[structopt(short = "-e", long = "fail-early")]
    fail_early: bool,

    /// Show progress
    #[structopt(long)]
    progress: bool,

    /// Preserve additional file attributes: file owner, group, setuid, setgid, mtime and atime
    #[structopt(short, long)]
    preserve: bool,

    /// Specify exactly what attributes to preserve.
    ///
    /// If specified, the "preserve" flag is ignored.
    ///
    /// The format is: "<type1>:<attributes1> <type2>:<attributes2> ..."
    /// Where <type> is one of: "f" (file), "d" (directory), "l" (symlink)
    /// And <attributes> is a comma separated list of: "uid", "gid", "time", <mode mask>
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

    /// File copy read buffer size
    #[structopt(long, default_value = "128KiB")]
    read_buffer: String,
}

#[instrument]
async fn async_main(args: Args) -> Result<CopySummary> {
    if args.paths.len() < 2 {
        return Err(anyhow::anyhow!(
            "You must specify at least one source and destination path!"
        ));
    }
    let src_strings = &args.paths[0..args.paths.len() - 1];
    for src in src_strings {
        if src == "." || src.ends_with("/.") {
            return Err(anyhow::anyhow!(
                "expanding source directory ({:?}) using dot operator ('.') is not supported, please use absolute path or '*' instead",
                std::path::PathBuf::from(src)
            ));
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
                Ok((src_path.to_owned(), dst_dir.join(src_file)))
            })
            .collect::<Result<Vec<(std::path::PathBuf, std::path::PathBuf)>>>()?
    } else {
        if src_strings.len() > 1 {
            return Err(anyhow::anyhow!(
                "Multiple sources can only be copied to a directory; if this is your intent follow the destination path with a trailing slash"
            ));
        }
        assert_eq!(src_strings.len(), 1);
        vec![(
            std::path::PathBuf::from(src_strings[0].clone()),
            std::path::PathBuf::from(dst_string),
        )]
    };
    let read_buffer = args
        .read_buffer
        .parse::<bytesize::ByteSize>()
        .unwrap()
        .as_u64() as usize;
    let mut join_set = tokio::task::JoinSet::new();
    let settings = common::CopySettings {
        read_buffer,
        dereference: args.dereference,
        fail_early: args.fail_early,
        overwrite: args.overwrite,
        overwrite_compare: common::parse_metadata_cmp_settings(&args.overwrite_compare)?,
    };
    event!(Level::DEBUG, "copy settings: {:?}", &settings);
    if args.preserve_settings.is_some() && args.preserve {
        event!(
            Level::WARN,
            "The --preserve flag is ignored when --preserve-settings is specified!"
        );
    }
    let preserve = if let Some(preserve_settings) = args.preserve_settings {
        common::parse_preserve_settings(&preserve_settings)?
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
    let mut copy_summary = CopySummary::default();
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(summary) => copy_summary = copy_summary + summary,
            Err(error) => {
                event!(Level::ERROR, "{}", &error);
                if args.fail_early {
                    return Err(error);
                }
                success = false;
            }
        }
    }
    if !success {
        return Err(anyhow::anyhow!("rcp encountered errors"));
    }
    Ok(copy_summary)
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    common::run(
        if args.progress { Some("copy") } else { None },
        args.quiet,
        args.verbose,
        args.summary,
        args.max_workers,
        args.max_blocking_threads,
        func,
    )?;
    Ok(())
}
