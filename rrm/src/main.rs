use anyhow::Result;
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "rrm")]
struct Args {
    /// Exit on first error
    #[structopt(short = "-e", long = "fail-early")]
    _fail_early: bool, // TODO: implement

    /// Show progress
    #[structopt(short, long)]
    progress: bool,

    /// Verbose level: -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    verbose: u8,

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    quiet: bool,

    /// Source path(s) and destination path
    #[structopt()]
    paths: Vec<std::path::PathBuf>,

    /// Number of worker threads, 0 means number of cores
    #[structopt(long, default_value = "0")]
    max_workers: usize,
}

async fn async_main(args: Args) -> Result<()> {
    let mut join_set = tokio::task::JoinSet::new();
    for path in args.paths {
        let settings = common::RmSettings {
            fail_early: args._fail_early,
        };
        let do_rm = || async move { common::rm(args.progress, &path, &settings).await };
        join_set.spawn(do_rm());
    }
    let mut errors = vec![];
    while let Some(res) = join_set.join_next().await {
        if let Err(error) = res? {
            errors.push(error);
        }
    }
    if !errors.is_empty() {
        return Err(anyhow::anyhow!("{:?}", &errors));
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::from_args();
    let func = {
        let args = args.clone();
        || async_main(args)
    };
    common::run(args.quiet, args.verbose, args.max_workers, func)?;
    Ok(())
}
