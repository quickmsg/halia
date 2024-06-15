use std::{
    fmt::Display,
    fs::Permissions,
    io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use uuid::Uuid;

pub mod device;
pub mod group;
pub mod point;
pub mod rule;
pub mod sink;
pub mod source;

static ROOT_DIR: &str = "storage";
static DEVICE_DIR: &str = "device";
static SOURCE_DIR: &str = "source";
static SINK_DIR: &str = "sink";
static RULE_DIR: &str = "rule";
static DATA_FILE: &str = "data";
static DELIMITER: char = '|';

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

async fn insert(path: PathBuf, datas: &[(Uuid, String)]) -> Result<(), io::Error> {
    let mut file = OpenOptions::new().append(true).open(path).await?;
    for (id, data) in datas {
        file.write(format!("{}{}{}\n", id, DELIMITER, data).as_bytes())
            .await?;
    }
    file.flush().await
}

async fn create_dir(path: PathBuf) -> Result<(), io::Error> {
    fs::create_dir(&path).await?;
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(path.join(DATA_FILE))
        .await?;
    file.set_permissions(Permissions::from_mode(0o666)).await
}

async fn read(path: impl AsRef<Path>) -> Result<Vec<(Uuid, String)>, io::Error> {
    let mut file = OpenOptions::new().read(true).open(path).await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let mut result = vec![];
    for line in buf.split("\n") {
        if line.len() == 0 {
            break;
        }

        let pos = line.find(DELIMITER).expect("数据文件损坏");
        let id = &line[..pos];
        let data = &line[pos + 1..];
        let id = id
            .parse::<Uuid>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "数据文件损坏"))?;
        result.push((id, data.to_string()));
    }

    Ok(result)
}

async fn update(path: impl AsRef<Path>, id: Uuid, data: String) -> Result<(), io::Error> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let new_line = format!("{}{}{}\n", id, DELIMITER, data);
    let mut lines: Vec<&str> = buf.split("\n").collect();
    for line in lines.iter_mut() {
        let split_pos = line.find(DELIMITER).expect("数据文件损坏");
        if line[..split_pos].parse::<Uuid>().expect("文件") == id {
            *line = new_line.as_str();
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

async fn delete(path: impl AsRef<Path>, ids: &Vec<Uuid>) -> Result<(), io::Error> {
    let mut file = OpenOptions::new().read(true).open(&path).await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let mut lines: Vec<&str> = buf.split("\n").collect();
    lines.retain(|line| match line.find(DELIMITER) {
        Some(pos) => !ids.contains(&line[..pos].parse::<Uuid>().expect("文件")),
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
