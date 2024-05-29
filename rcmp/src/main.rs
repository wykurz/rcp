use anyhow::Result;
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "rcmp",
    about = "`rcmp` is a tool for comparing large filesets.

Currently, it only supports comparing metadata (no content checking)."
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

    /// Verbose level (implies "summary"): -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,

    /// Print summary at the end
    #[structopt(long)]
    summary: bool,

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// File where we store comparison mismatch output
    #[structopt()]
    log: std::path::PathBuf,

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
}

async fn async_main(args: Args) -> Result<common::CmpSummary> {
    let log_handle = common::LogWriter::new(&args.log).await?;
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
    common::run(
        if args.progress { Some("rcmp") } else { None },
        args.quiet,
        args.verbose,
        args.summary,
        args.max_workers,
        args.max_blocking_threads,
        func,
    )?;
    Ok(())
}
