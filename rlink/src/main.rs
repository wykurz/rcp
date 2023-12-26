use anyhow::Result;
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "rlink")]
struct Args {
    /// Exit on first error
    #[structopt(short = "-e", long = "fail-early")]
    fail_early: bool,

    /// Show progress
    #[structopt(long)]
    progress: bool,

    /// Always follow symbolic links in source
    #[structopt(short = "-L", long)]
    dereference: bool,

    /// Verbose level: -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Directory with contents we want to update into `dst`
    #[structopt()]
    src: std::path::PathBuf,

    /// Directory where we put either a hard-link of a file from `link` if it was unchanged, or a copy of a file from `new` if it's been modified
    #[structopt()]
    dst: std::path::PathBuf,

    /// Directory with updated contents of `link`
    #[structopt(long)]
    update: Option<std::path::PathBuf>,

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
    let mut inputs = vec![&args.src];
    if let Some(update) = &args.update {
        inputs.push(update);
    }
    for path in inputs {
        if !tokio::fs::metadata(path).await?.is_dir() {
            return Err(anyhow::anyhow!(
                "Input paths must be directories, but {:?} is not a directory",
                path
            ));
        }
    }
    if tokio::fs::metadata(&args.dst).await.is_ok() {
        return Err(anyhow::anyhow!(
            "Destination path must not exist but {:?} is present",
            &args.dst
        ));
    }
    let read_buffer = args
        .read_buffer
        .parse::<bytesize::ByteSize>()
        .unwrap()
        .as_u64() as usize;
    common::link(
        &args.src,
        &args.dst,
        &args.update,
        &common::CopySettings {
            preserve: true, // ALWAYS preserve metadata
            read_buffer,
            dereference: args.dereference,
            fail_early: args.fail_early,
        },
    )
    .await
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    common::run(
        if args.progress { Some("rlink") } else { None },
        args.quiet,
        args.verbose,
        args.max_workers,
        args.max_blocking_threads,
        func,
    )?;
    Ok(())
}
