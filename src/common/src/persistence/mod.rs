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

pub mod local;

use crate::error::HaliaResult;

static ROOT_DIR: &str = "storage";
pub static DELIMITER: char = '|';
static DEVICE_FILE: &str = "devices";
static APP_FILE: &str = "apps";
static SOURCE_FILE: &str = "sources";
static SINK_FILE: &str = "sinks";
static RULE_FILE: &str = "rules";

#[derive(Debug, PartialEq)]
pub enum Status {
    Stopped = 0,
    Runing = 1,
}

pub struct Device {
    id: String,
    status: bool,
    conf: String,
}

pub struct SourceOrSink {
    id: String,
    // device or app id
    parent_id: String,
    conf: String,
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



pub async fn init_dir() -> Result<(), io::Error> {
    if !Path::new(ROOT_DIR).exists() {
        fs::create_dir(ROOT_DIR).await?;
    }

    Ok(())
}

pub async fn init_devices() -> Result<(), io::Error> {
    create_file(get_device_file_path()).await
}

pub async fn init_apps() -> Result<(), io::Error> {
    create_file(get_app_file_path()).await
}

pub async fn init_rules() -> Result<(), io::Error> {
    create_file(get_rule_file_path()).await
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

fn get_device_file_path() -> PathBuf {
    Path::new(ROOT_DIR).join(DEVICE_FILE)
}

pub async fn create_device(device_id: &Uuid, data: &String) -> Result<(), io::Error> {
    create(
        get_device_file_path(),
        device_id,
        &format!("{}{}{}", Status::Stopped, DELIMITER, data,),
    )
    .await?;

    fs::create_dir(Path::new(ROOT_DIR).join(device_id.to_string())).await?;
    create_file(get_source_file_path(device_id)).await?;
    create_file(get_sink_file_path(device_id)).await
}

pub async fn read_devices() -> Result<Vec<String>, io::Error> {
    read(get_device_file_path()).await
}

pub async fn update_device_conf(device_id: &Uuid, conf: &String) -> Result<(), io::Error> {
    update_segment(get_device_file_path(), device_id, 2, conf).await
}

pub async fn update_device_status(device_id: &Uuid, status: Status) -> Result<(), io::Error> {
    update_segment(get_device_file_path(), device_id, 1, &status.to_string()).await
}

pub async fn delete_device(device_id: &Uuid) -> Result<(), io::Error> {
    delete(get_device_file_path(), device_id).await?;
    fs::remove_dir_all(Path::new(ROOT_DIR).join(device_id.to_string())).await
}

fn get_app_file_path() -> PathBuf {
    Path::new(ROOT_DIR).join(APP_FILE)
}

pub async fn create_app(app_id: &Uuid, data: &String) -> Result<(), io::Error> {
    create(
        get_app_file_path(),
        app_id,
        &format!("{}{}{}", Status::Stopped, DELIMITER, data,),
    )
    .await?;

    fs::create_dir(Path::new(ROOT_DIR).join(app_id.to_string())).await?;
    create_file(get_source_file_path(app_id)).await?;
    create_file(get_sink_file_path(app_id)).await
}

pub async fn read_apps() -> Result<Vec<String>, io::Error> {
    read(get_app_file_path()).await
}

pub async fn update_app_conf(app_id: &Uuid, conf: &String) -> Result<(), io::Error> {
    update_segment(get_app_file_path(), app_id, 2, conf).await
}

pub async fn update_app_status(app_id: &Uuid, status: Status) -> Result<(), io::Error> {
    update_segment(get_app_file_path(), app_id, 1, &status.to_string()).await
}

pub async fn delete_app(app_id: &Uuid) -> Result<(), io::Error> {
    delete(get_app_file_path(), app_id).await?;
    fs::remove_dir_all(Path::new(ROOT_DIR).join(app_id.to_string())).await
}

fn get_source_file_path(id: &Uuid) -> PathBuf {
    Path::new(ROOT_DIR).join(id.to_string()).join(SOURCE_FILE)
}

pub async fn create_source(id: &Uuid, source_id: &Uuid, data: &String) -> Result<(), io::Error> {
    create(get_source_file_path(id), source_id, data).await
}

pub async fn read_sources(id: &Uuid) -> Result<Vec<String>, io::Error> {
    read(get_source_file_path(id)).await
}

pub async fn update_source(id: &Uuid, source_id: &Uuid, data: &String) -> Result<(), io::Error> {
    update(get_source_file_path(id), source_id, data).await
}

pub async fn delete_source(id: &Uuid, source_id: &Uuid) -> Result<(), io::Error> {
    delete(get_source_file_path(id), source_id).await
}

fn get_sink_file_path(id: &Uuid) -> PathBuf {
    Path::new(ROOT_DIR).join(id.to_string()).join(SINK_FILE)
}

pub async fn create_sink(id: &Uuid, sink_id: &Uuid, data: &String) -> Result<(), io::Error> {
    create(get_sink_file_path(id), sink_id, data).await
}

pub async fn read_sinks(id: &Uuid) -> Result<Vec<String>, io::Error> {
    read(get_sink_file_path(id)).await
}

pub async fn update_sink(id: &Uuid, sink_id: &Uuid, data: &String) -> Result<(), io::Error> {
    update(get_sink_file_path(id), sink_id, &data).await
}

pub async fn delete_sink(id: &Uuid, sink_id: &Uuid) -> Result<(), io::Error> {
    delete(get_sink_file_path(id), sink_id).await
}

fn get_rule_file_path() -> PathBuf {
    Path::new(ROOT_DIR).join(RULE_FILE)
}

pub async fn create_rule(rule_id: &Uuid, data: &String) -> Result<(), io::Error> {
    create(
        get_rule_file_path(),
        rule_id,
        &format!("{}{}{}", Status::Stopped, DELIMITER, data,),
    )
    .await
}

pub async fn read_rules() -> Result<Vec<String>, io::Error> {
    read(get_rule_file_path()).await
}

pub async fn update_rule_conf(rule_id: &Uuid, conf: &String) -> Result<(), io::Error> {
    update_segment(get_rule_file_path(), rule_id, 2, conf).await
}

pub async fn update_rule_status(app_id: &Uuid, status: Status) -> Result<(), io::Error> {
    update_segment(get_rule_file_path(), app_id, 1, &status.to_string()).await
}

pub async fn delete_rule(rule_id: &Uuid) -> Result<(), io::Error> {
    delete(get_rule_file_path(), rule_id).await
}
