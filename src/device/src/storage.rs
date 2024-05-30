use std::{io, path::Path};

use serde::Deserialize;
use serde_json::Value;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use tracing::error;

pub(crate) async fn insert(path: impl AsRef<Path>, record: &[u8]) -> Result<(), io::Error> {
    let mut file = match fs::try_exists(&path).await? {
        true => OpenOptions::new().append(true).open(&path).await?,
        false => File::create(&path).await?,
    };

    file.write_all(record).await?;
    file.flush().await
}

pub(crate) async fn create_dir(path: impl AsRef<Path>) -> Result<(), io::Error> {
    match fs::try_exists(&path).await? {
        true => Ok(()),
        false => fs::create_dir_all(&path).await,
    }
}

pub(crate) async fn read_file(path: impl AsRef<Path>) -> Result<(), io::Error> {
    todo!()
}

pub(crate) async fn update(
    path: impl AsRef<Path>,
    record: &[u8],
    id: u64,
) -> Result<(), io::Error> {
    todo!()
}

#[derive(Deserialize)]
struct Record {
    id: u64,
    req: Value,
}

pub(crate) async fn delete(path: impl AsRef<Path>, id: u64) -> Result<(), io::Error> {
    let mut file = File::open(&path).await?;

    let mut contents = String::new();
    file.read_to_string(&mut contents).await?;

    let mut lines: Vec<&str> = contents.lines().collect();
    lines.retain(|line| match serde_json::from_str::<Record>(line) {
        Ok(record) => record.id != id,
        Err(_) => {
            error!("文件已损坏");
            true
        }
    });

    let new_contents = lines.join("\n");

    let mut file = File::create(&path).await?;
    file.write_all(new_contents.as_bytes()).await?;
    file.flush().await?;

    Ok(())
}
