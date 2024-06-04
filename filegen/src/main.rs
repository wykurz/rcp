use anyhow::{Context, Result};
use async_recursion::async_recursion;
use rand::Rng;
use structopt::StructOpt;
use tokio::io::AsyncWriteExt;

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
#[structopt(name = "filegen")]
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

    /// Size of the buffer used to write to each file. Accepts suffixes like "1K", "1M", "1G"
    #[structopt(default_value = "4K")]
    bufsize: String,
}

async fn write_file(path: std::path::PathBuf, mut filesize: usize, bufsize: usize) -> Result<()> {
    let mut bytes = vec![0u8; bufsize];
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .await
        .context(format!("Error opening {:?}", &path))?;
    while filesize > 0 {
        {
            // make sure rng falls out of scope before await
            let mut rng = rand::thread_rng();
            rng.fill(&mut bytes[..]);
        }
        let writesize = std::cmp::min(filesize, bufsize) as usize;
        file.write_all(&bytes[..writesize])
            .await
            .context(format!("Error writing to {:?}", &path))?;
        filesize -= writesize;
    }
    Ok(())
}

#[async_recursion]
async fn filegen(
    root: &std::path::Path,
    dirwidth: &[usize],
    numfiles: usize,
    filesize: usize,
    writebuf: usize,
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
            filegen(&path, &dirwidth, numfiles, filesize, writebuf).await
        };
        join_set.spawn(recurse());
    }
    // generate files
    for i in 0..numfiles {
        let path = root.join(format!("file{}", i));
        join_set.spawn(write_file(path.clone(), filesize, writebuf));
    }
    while let Some(res) = join_set.join_next().await {
        res??
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::from_args();
    let filesize = args
        .filesize
        .parse::<bytesize::ByteSize>()
        .unwrap()
        .as_u64() as usize;
    let writebuf = args.bufsize.parse::<bytesize::ByteSize>().unwrap().as_u64() as usize;
    let root = args.root.join("filegen");
    tokio::fs::create_dir(&root)
        .await
        .context(format!("Error creating {:?}", &root))?;
    filegen(
        &root,
        &args.dirwidth.value,
        args.numfiles,
        filesize,
        writebuf,
    )
    .await?;
    Ok(())
}
