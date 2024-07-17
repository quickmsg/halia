use std::{
    io,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use tracing::debug;
use uuid::Uuid;

use super::{Status, DATA_FILE, DELIMITER, ROOT_DIR};

fn get_dir() -> PathBuf {
    Path::new(super::ROOT_DIR).join(super::RULE_DIR)
}

fn get_file_path() -> PathBuf {
    get_dir().join(super::DATA_FILE)
}

pub async fn init() -> Result<(), io::Error> {
    fs::create_dir_all(get_dir()).await?;
    super::create_file(get_file_path()).await
}

pub async fn create(id: &Uuid, data: String) -> Result<(), io::Error> {
    let data = format!("{}{}{}", Status::Stopped, DELIMITER, data,);
    super::create(get_file_path(), id, &data).await
}

pub async fn read() -> Result<Vec<String>, io::Error> {
    super::read(get_file_path()).await
}

pub async fn update_conf(id: Uuid, data: String) -> Result<(), io::Error> {
    let path = Path::new(ROOT_DIR).join(DATA_FILE);
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

pub async fn update_status(id: Uuid, data: String) -> Result<(), io::Error> {
    let path = Path::new(ROOT_DIR).join(DATA_FILE);
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

pub async fn delete(id: &Uuid) -> Result<(), io::Error> {
    super::delete(Path::new(ROOT_DIR).join(DATA_FILE), id).await?;
    fs::remove_dir_all(Path::new(ROOT_DIR).join(id.to_string())).await
}
