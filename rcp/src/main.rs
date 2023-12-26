use anyhow::{Context, Result};
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "rcp")]
struct Args {
    /// Overwrite existing files/directories
    #[structopt(short, long)]
    overwrite: bool,

    /// Exit on first error
    #[structopt(short = "-e", long = "fail-early")]
    fail_early: bool,

    /// Show progress
    #[structopt(long)]
    progress: bool,

    /// Preserve additional file attributes: file owner, group, setuid, setgid, mtime and atime
    #[structopt(short, long)]
    preserve: bool,

    /// Always follow symbolic links in source
    #[structopt(short = "-L", long)]
    dereference: bool,

    /// Verbose level: -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,

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

async fn async_main(args: Args) -> Result<()> {
    if args.paths.len() < 2 {
        return Err(anyhow::anyhow!(
            "You must specify at least one source and destination path!"
        ));
    }
    let src_strings = &args.paths[0..args.paths.len() - 1];
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
                    .context(format!("source {:?} is not a file", &src_path))
                    .unwrap();
                (src_path.to_owned(), dst_dir.join(src_file))
            })
            .collect()
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
    for (src_path, dst_path) in src_dst {
        let do_copy = || async move {
            if dst_path.exists() {
                if args.overwrite {
                    // TODO: is this the right behavior?
                    if tokio::fs::metadata(&dst_path).await?.is_file() {
                        tokio::fs::remove_file(&dst_path).await?;
                    } else {
                        tokio::fs::remove_dir_all(&dst_path).await?;
                    }
                } else {
                    return Err(anyhow::anyhow!(
                        "destination {:?} already exists, use --overwrite to overwrite",
                        dst_path
                    ));
                }
            }
            common::copy(
                &src_path,
                &dst_path,
                &common::CopySettings {
                    preserve: args.preserve,
                    read_buffer,
                    dereference: args.dereference,
                    fail_early: args.fail_early,
                },
            )
            .await
        };
        join_set.spawn(do_copy());
    }
    let mut success = true;
    while let Some(res) = join_set.join_next().await {
        if let Err(error) = res? {
            log::error!("{}", &error);
            if args.fail_early {
                return Err(error);
            }
            success = false;
        }
    }
    if !success {
        return Err(anyhow::anyhow!("rcp encountered errors"));
    }
    Ok(())
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
        args.max_workers,
        args.max_blocking_threads,
        func,
    )?;
    Ok(())
}
