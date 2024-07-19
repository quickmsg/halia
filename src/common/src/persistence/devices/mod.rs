use std::{
    io,
    path::{Path, PathBuf},
};

use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use uuid::Uuid;

use super::{Status, DELIMITER};

static DEVICE_FILE: &str = "devices";

pub mod coap;
pub mod modbus;
pub mod opcua;

pub(crate) fn get_dir() -> PathBuf {
    Path::new(super::ROOT_DIR).join(super::DEVICE_DIR)
}

pub(crate) fn get_device_dir() -> PathBuf {
    Path::new(super::ROOT_DIR).join(super::DEVICE_DIR)
}

pub(crate) fn get_device_file() -> PathBuf {
    get_dir().join(DEVICE_FILE)
}

pub(crate) fn get_device_file_path() -> PathBuf {
    get_dir().join(DEVICE_FILE)
}

pub async fn init() -> Result<(), io::Error> {
    fs::create_dir_all(get_dir()).await?;
    super::create_file(get_dir().join(DEVICE_FILE)).await
}

pub async fn insert_coap_device(device_id: &Uuid, data: &String) -> Result<(), io::Error> {
    insert_device(device_id, data).await?;
    let base_dir = get_dir().join(device_id.to_string());
    super::create_file(base_dir.join("paths")).await?;
    super::create_file(base_dir.join("sources")).await?;
    super::create_file(base_dir.join("sinks")).await
}

pub async fn insert_coap_path(
    device_id: &Uuid,
    path_id: &Uuid,
    data: &String,
) -> Result<(), io::Error> {
    super::insert(
        get_dir().join(device_id.to_string()).join("paths"),
        path_id,
        data,
    )
    .await
}

pub async fn read_coap_paths(device_id: &Uuid) -> Result<Vec<String>, io::Error> {
    super::read(get_dir().join(device_id.to_string()).join("paths")).await
}

pub async fn insert_modbus_device(device_id: &Uuid, data: &String) -> Result<(), io::Error> {
    insert_device(device_id, data).await?;
    let base_dir = get_dir().join(device_id.to_string());
    super::create_file(base_dir.join("groups")).await?;
    super::create_file(base_dir.join("sinks")).await
}

async fn insert_device(device_id: &Uuid, data: &String) -> Result<(), io::Error> {
    super::insert(
        get_device_file(),
        device_id,
        &format!("{}{}{}", Status::Stopped, DELIMITER, data),
    )
    .await?;
    fs::create_dir_all(get_dir().join(device_id.to_string())).await
    // super::create_file(get_group_file(device_id)).await?;
    // super::create_file(get_sink_file(device_id)).await
}

pub async fn read_devices() -> Result<Vec<String>, io::Error> {
    super::read(get_device_file()).await
}

pub async fn update_device_conf(id: &Uuid, conf: String) -> Result<(), io::Error> {
    let path = get_device_file();
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let new_line;
    let mut lines: Vec<&str> = buf.split("\n").collect();
    let id_str = id.to_string();
    for line in lines.iter_mut() {
        if line[..36] == id_str {
            let fields: Vec<&str> = line.split(DELIMITER).collect();
            assert_eq!(fields.len(), 4);

            new_line = format!(
                "{}{}{}{}{}{}{}",
                fields[0], DELIMITER, fields[1], DELIMITER, fields[2], DELIMITER, conf,
            );
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

pub async fn update_device_status(id: &Uuid, status: Status) -> Result<(), io::Error> {
    let path = get_device_file();
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
        if fields.len() != 4 {
            panic!("数据文件损坏");
        }
        if fields[0].parse::<Uuid>().expect("数据文件损坏") == *id {
            new_line = format!(
                "{}{}{}{}{}{}{}",
                fields[0],
                DELIMITER,
                fields[1],
                DELIMITER,
                status.to_string(),
                DELIMITER,
                fields[3]
            );
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

pub async fn delete_device(id: &Uuid) -> Result<(), io::Error> {
    super::delete(get_device_file(), id).await?;
    fs::remove_dir_all(get_dir().join(id.to_string())).await
}
