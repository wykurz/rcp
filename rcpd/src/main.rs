use anyhow::{anyhow, Context, Result};
use common::ProgressType;
use structopt::StructOpt;
use tracing::{event, instrument, Level};

#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "rcpd",
    about = "`rcpd` is used by the `rcp` command for performing remote data copies. Please see `rcp` for more \
information."
)]
struct Args {
    /// Number of worker threads, 0 means number of cores
    #[structopt(long, default_value = "0")]
    max_workers: usize,

    /// Number of blocking worker threads, 0 means Tokio runtime default (512)
    #[structopt(long, default_value = "0")]
    max_blocking_threads: usize,

    /// Maximum number of open files, 0 means no limit, leaving unspecified means using 80% of max open files system
    /// limit
    #[structopt(long)]
    max_open_files: Option<usize>,

    /// Throttle the number of opearations per second, 0 means no throttle
    #[structopt(long, default_value = "0")]
    ops_throttle: usize,
}

fn main() -> Result<(), anyhow::Error> {
    Ok(())
}
