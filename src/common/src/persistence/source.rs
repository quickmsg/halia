use std::{
    io,
    path::{Path, PathBuf},
};

use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncWriteExt},
};
use tracing::debug;
use uuid::Uuid;

use super::{Status, DELIMITER};

fn get_file() -> PathBuf {
    Path::new(super::ROOT_DIR)
        .join(super::SOURCE_DIR)
        .join(super::DATA_FILE)
}

pub async fn insert(id: Uuid, data: String) -> Result<(), io::Error> {
    super::insert(get_file(), &vec![(id, data)]).await
}

pub async fn read() -> Result<Vec<(Uuid, String)>, io::Error> {
    super::read(get_file()).await
}

pub async fn update(id: Uuid, data: String) -> Result<(), io::Error> {
    let path = get_file();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let new_line;
    let mut lines: Vec<&str> = buf.split("\n").collect();
    for line in lines.iter_mut() {
        let fields: Vec<&str> = line.split(DELIMITER).collect();
        if fields.len() != 3 {
            panic!("数据文件损坏");
        }
        if fields[0].parse::<Uuid>().expect("数据文件损坏") == id {
            new_line = format!(
                "{}{}{}{}{}",
                fields[0], DELIMITER, fields[1], DELIMITER, data
            );
            debug!("{}", new_line);
            *line = new_line.as_str();
            break;
        }
    }

    let buf = lines.join("\n");
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .await?;
    file.write_all(buf.as_bytes()).await?;
    Ok(())
}

pub async fn delete(id: Uuid) -> Result<(), io::Error> {
    super::delete(get_file(), &vec![id]).await
}
