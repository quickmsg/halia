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

use super::DELIMITER;

static SOURCE_FILE: &str = "source";
static SINK_FILE: &str = "sink";

fn get_dir() -> PathBuf {
    Path::new(super::ROOT_DIR).join(super::APP_DIR)
}

fn get_file() -> PathBuf {
    get_dir().join(super::DATA_FILE)
}

fn get_source_file(connector_id: &Uuid) -> PathBuf {
    get_dir().join(connector_id.to_string()).join(SOURCE_FILE)
}

fn get_sink_file(connector_id: &Uuid) -> PathBuf {
    get_dir().join(connector_id.to_string()).join(SINK_FILE)
}

pub async fn init() -> Result<(), io::Error> {
    fs::create_dir_all(get_dir()).await?;
    super::create_file(get_file()).await
}

pub async fn insert_connector(id: &Uuid, data: &Bytes) -> Result<(), io::Error> {
    unsafe {
        super::insert(get_file(), id, std::str::from_utf8_unchecked(data)).await?;
    }
    fs::create_dir(get_dir().join(id.to_string())).await?;
    super::create_file(get_dir().join(id.to_string()).join(SOURCE_FILE)).await?;
    super::create_file(get_dir().join(id.to_string()).join(SINK_FILE)).await
}

pub async fn insert_source(
    connector_id: &Uuid,
    source_id: &Uuid,
    data: &Bytes,
) -> Result<(), io::Error> {
    unsafe {
        super::insert(
            get_source_file(connector_id),
            source_id,
            std::str::from_utf8_unchecked(data),
        )
        .await
    }
}

pub async fn insert_sink(
    connector_id: &Uuid,
    sink_id: &Uuid,
    data: &Bytes,
) -> Result<(), io::Error> {
    unsafe {
        super::insert(
            get_sink_file(connector_id),
            sink_id,
            std::str::from_utf8_unchecked(data),
        )
        .await
    }
}

pub async fn read_connectors() -> Result<Vec<(Uuid, String)>, io::Error> {
    super::read(get_file()).await
}

pub async fn read_sources(connector_id: &Uuid) -> Result<Vec<(Uuid, String)>, io::Error> {
    super::read(get_dir().join(connector_id.to_string()).join(SOURCE_FILE)).await
}

pub async fn read_sinks(connector_id: &Uuid) -> Result<Vec<(Uuid, String)>, io::Error> {
    super::read(get_dir().join(connector_id.to_string()).join(SINK_FILE)).await
}

pub async unsafe fn update(id: &Uuid, conf: &Bytes) -> Result<(), io::Error> {
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
        if fields[0].parse::<Uuid>().expect("数据文件损坏") == *id {
            new_line = format!(
                "{}{}{}{}{}",
                fields[0],
                DELIMITER,
                fields[1],
                DELIMITER,
                std::str::from_utf8_unchecked(conf)
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
    super::delete(get_file(), id).await?;
    fs::remove_dir_all(get_dir().join(id.to_string())).await
}
