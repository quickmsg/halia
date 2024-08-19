use std::{
    fmt::Display,
    fs::Permissions,
    io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncWriteExt},
};
use uuid::Uuid;

pub mod apps;
pub mod devices;
pub mod message;
pub mod rule;

static ROOT_DIR: &str = "storage";
static RULE_DIR: &str = "rules";
pub static DELIMITER: char = '|';

#[derive(Debug, PartialEq)]
pub enum Status {
    Stopped = 0,
    Runing = 1,
}

impl Status {
    fn to_string(&self) -> String {
        match self {
            Status::Stopped => "0".to_string(),
            Status::Runing => "1".to_string(),
        }
    }
}

impl Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Stopped => write!(f, "0"),
            Status::Runing => write!(f, "1"),
        }
    }
}

async fn insert(path: PathBuf, id: &Uuid, data: &str) -> Result<(), io::Error> {
    let mut file = OpenOptions::new().append(true).open(path).await?;
    file.write(format!("{}{}{}\n", id, DELIMITER, data).as_bytes())
        .await?;
    file.flush().await
}

pub async fn create(path: PathBuf, id: &Uuid, data: &str) -> Result<(), io::Error> {
    let mut file = OpenOptions::new().append(true).open(path).await?;
    file.write(format!("{}{}{}\n", id, DELIMITER, data).as_bytes())
        .await?;
    file.flush().await
}

async fn create_file(path: PathBuf) -> Result<(), io::Error> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(path)
        .await?;
    file.set_permissions(Permissions::from_mode(0o666)).await
}

async fn read(path: impl AsRef<Path>) -> Result<Vec<String>, io::Error> {
    let mut file = OpenOptions::new().read(true).open(path).await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    Ok(buf
        .split("\n")
        .filter(|s| s.len() > 0)
        .map(|s| s.to_string())
        .collect())
}

async fn update(path: impl AsRef<Path>, id: &Uuid, data: &str) -> Result<(), io::Error> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let new_line = format!("{}{}{}", id, DELIMITER, data);
    let mut lines: Vec<&str> = buf.split("\n").collect();
    for line in lines.iter_mut() {
        if line.len() > 0 {
            let split_pos = line.find(DELIMITER).expect("数据文件损坏");
            if line[..split_pos].parse::<Uuid>().expect("文件") == *id {
                *line = new_line.as_str();
            }
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

async fn update_segment(
    path: impl AsRef<Path>,
    id: &Uuid,
    pos: usize,
    data: &str,
) -> Result<(), io::Error> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .await?;

    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let mut new_line = String::new();
    let mut lines: Vec<&str> = buf.split("\n").collect();
    let id_str = id.to_string();
    for line in lines.iter_mut() {
        if line.len() == 0 {
            continue;
        }
        if line.starts_with(&id_str) {
            let fields: Vec<&str> = line.split(DELIMITER).collect();
            for n in 0..pos {
                new_line.push_str(fields[n]);
                new_line.push(DELIMITER);
            }

            new_line.push_str(data);

            if pos < fields.len() {
                for n in pos + 1..fields.len() {
                    new_line.push(DELIMITER);
                    new_line.push_str(fields[n]);
                }
            }

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

async fn delete(path: impl AsRef<Path>, id: &Uuid) -> Result<(), io::Error> {
    let mut buf = String::new();
    {
        let mut file = OpenOptions::new().read(true).open(&path).await?;
        file.read_to_string(&mut buf).await?;
    }

    let mut lines: Vec<&str> = buf.split("\n").collect();
    let id = id.to_string();
    lines.retain(|line| match line.find(DELIMITER) {
        Some(pos) => id != &line[..pos],
        None => false,
    });

    let buf = lines.join("\n");
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .await?;

    file.write(buf.as_bytes()).await?;
    file.flush().await
}
