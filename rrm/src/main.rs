use anyhow::Result;
use structopt::StructOpt;
use tracing::{event, instrument, Level};

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "rrm",
    about = "`rrm` is a simple tool for removing large numbers of files.

Note the basic usage is equivalent to `rm -rf`."
)]
struct Args {
    /// Exit on first error
    #[structopt(short = "-e", long = "fail-early")]
    fail_early: bool,

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

    /// Source path(s) and destination path
    #[structopt()]
    paths: Vec<std::path::PathBuf>,

    /// Number of worker threads, 0 means number of cores
    #[structopt(long, default_value = "0")]
    max_workers: usize,

    /// Number of blocking worker threads, 0 means Tokio runtime default (512)
    #[structopt(long, default_value = "0")]
    max_blocking_threads: usize,
}

#[instrument]
async fn async_main(args: Args) -> Result<common::RmSummary> {
    let mut join_set = tokio::task::JoinSet::new();
    for path in args.paths {
        let settings = common::RmSettings {
            fail_early: args.fail_early,
        };
        let do_rm = || async move { common::rm(&path, &settings).await };
        join_set.spawn(do_rm());
    }
    let mut success = true;
    let mut rm_summary = common::RmSummary::default();
    while let Some(res) = join_set.join_next().await {
        match res? {
            Ok(summary) => rm_summary = rm_summary + summary,
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
        return Err(anyhow::anyhow!("rrm encountered errors"));
    }
    Ok(rm_summary)
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    common::run(
        if args.progress { Some("rm") } else { None },
        args.quiet,
        args.verbose,
        args.summary,
        args.max_workers,
        args.max_blocking_threads,
        func,
    )?;
    Ok(())
}
