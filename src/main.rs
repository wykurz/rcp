#[macro_use]
extern crate log;

use anyhow::Result;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "rcp")]
struct Args {
    /// Output file
    #[structopt(short, long, parse(from_os_str))]
    src: std::path::PathBuf,

    /// Output file
    #[structopt(short, long, parse(from_os_str))]
    dst: std::path::PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::from_args();
    info!("Copy: {:?} -> {:?}", args.src, args.dst);
    Ok(())
}
