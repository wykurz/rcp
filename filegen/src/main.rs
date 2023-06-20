use anyhow::{Context, Result};
use async_recursion::async_recursion;
use rand::Rng;
use structopt::StructOpt;

#[derive(Debug)]
struct Dirwidth {
    value: Vec<usize>,
}

impl std::str::FromStr for Dirwidth {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let value = s
            .split(',')
            .map(|s| s.parse::<usize>())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Dirwidth { value })
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "rcp")]
struct Args {
    /// Root directory where files are generated
    #[structopt(parse(from_os_str))]
    root: std::path::PathBuf,

    /// Number of sub-directories in the generated directory tree. E.g., "3,2" will generate:
    /// |- d1
    ///    |- d1a
    ///    |- d1b
    /// |- d2
    ///    |- d2a
    ///    |- d2b
    /// |- d3
    ///    |- d3a
    ///    |- d3b
    #[structopt()]
    dirwidth: Dirwidth,

    /// Number of files in each directory
    #[structopt()]
    numfiles: usize,

    /// Size of each file. Accepts suffixes like "1K", "1M", "1G"
    #[structopt()]
    filesize: String,
}

#[async_recursion]
async fn filegen(
    root: &std::path::Path,
    dirwidth: &[usize],
    numfiles: usize,
    filesize: u64,
) -> Result<()> {
    let numdirs = *dirwidth.first().unwrap_or(&0);
    let mut join_set = tokio::task::JoinSet::new();
    // generate directories and recurse into them
    for i in 0..numdirs {
        let path = root.join(format!("dir{}", i));
        let dirwidth = dirwidth[1..].to_owned();
        let recurse = || async move {
            tokio::fs::create_dir(&path)
                .await
                .map_err(anyhow::Error::msg)?;
            filegen(&path, &dirwidth, numfiles, filesize).await
        };
        join_set.spawn(recurse());
    }
    // generate files
    for i in 0..numfiles {
        let path = root.join(format!("file{}", i));
        let mut rng = rand::thread_rng();
        let mut bytes = vec![0u8; filesize as usize];
        rng.fill(&mut bytes[..]);
        let create_file = || async move {
            tokio::fs::write(path, &bytes)
                .await
                .map_err(anyhow::Error::msg)
        };
        join_set.spawn(create_file());
    }
    while let Some(res) = join_set.join_next().await {
        res??
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::from_args();
    let filesize = args
        .filesize
        .parse::<bytesize::ByteSize>()
        .unwrap()
        .as_u64();
    let root = args.root.join("filegen");
    tokio::fs::create_dir(&root)
        .await
        .context(format!("Error creating {:?}", &root))?;
    filegen(&root, &args.dirwidth.value, args.numfiles, filesize).await?;
    Ok(())
}
