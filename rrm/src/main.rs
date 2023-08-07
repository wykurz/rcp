#[macro_use]
extern crate log;

use anyhow::Result;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "rcp")]
struct Args {
    /// Exit on first error
    #[structopt(short = "-e", long = "fail-early")]
    _fail_early: bool, // TODO: implement

    /// Show progress
    #[structopt(short, long)]
    progress: bool,

    /// Verbose level: -v INFO / -vv DEBUG / -vvv TRACE (default: ERROR))
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    _verbose: u8, // TODO: implement

    /// Quiet mode, don't report errors
    #[structopt(short = "q", long = "quiet")]
    _quiet: bool, // TODO: implement

    /// Source path(s) and destination path
    #[structopt()]
    paths: Vec<std::path::PathBuf>,

    /// Number of worker threads, 0 means number of cores
    #[structopt(long, default_value = "0")]
    _max_workers: usize, // TODO: implement
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::from_args();
    if !sysinfo::set_open_files_limit(isize::MAX) {
        info!("Failed to update the open files limit (expeted on non-linux targets)");
    }
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
