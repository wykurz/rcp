#[cfg(test)]
async fn create_temp_dir() -> anyhow::Result<std::path::PathBuf> {
    let mut idx = 0;
    loop {
        let tmp_dir = std::env::temp_dir().join(format!("rcp_test{}", &idx));
        if let Err(error) = tokio::fs::create_dir(&tmp_dir).await {
            match error.kind() {
                std::io::ErrorKind::AlreadyExists => {
                    idx += 1;
                }
                _ => return Err(error.into()),
            }
        } else {
            return Ok(tmp_dir);
        }
    }
}

#[cfg(test)]
pub async fn setup_test_dir() -> anyhow::Result<std::path::PathBuf> {
    // create a temporary directory
    let tmp_dir = create_temp_dir().await?;
    // foo
    // |- 0.txt
    // |- bar
    //    |- 1.txt
    //    |- 2.txt
    //    |- 3.txt
    // |- baz
    //    |- 4.txt
    //    |- 5.txt -> ../bar/2.txt
    //    |- 6.txt -> (absolute path) .../foo/bar/3.txt
    let foo_path = tmp_dir.join("foo");
    tokio::fs::create_dir(&foo_path).await.unwrap();
    tokio::fs::write(foo_path.join("0.txt"), "0").await.unwrap();
    let bar_path = foo_path.join("bar");
    tokio::fs::create_dir(&bar_path).await.unwrap();
    tokio::fs::write(bar_path.join("1.txt"), "1").await.unwrap();
    tokio::fs::write(bar_path.join("2.txt"), "2").await.unwrap();
    tokio::fs::write(bar_path.join("3.txt"), "3").await.unwrap();
    let baz_path = foo_path.join("baz");
    tokio::fs::create_dir(&baz_path).await.unwrap();
    tokio::fs::write(baz_path.join("4.txt"), "4").await.unwrap();
    tokio::fs::symlink("../bar/2.txt", baz_path.join("5.txt"))
        .await
        .unwrap();
    tokio::fs::symlink(bar_path.join("3.txt"), baz_path.join("6.txt"))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    Ok(tmp_dir)
}
