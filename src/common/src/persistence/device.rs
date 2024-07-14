use std::{
    io,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};
use uuid::Uuid;

use super::{Status, DELIMITER};

static DEVICE_FILE: &str = "devices";
static GROUP_FILE: &str = "groups";
static SINK_FILE: &str = "sinks";
static POINT_FILE: &str = "points";

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

fn get_group_file(device_id: &Uuid) -> PathBuf {
    get_dir().join(device_id.to_string()).join(GROUP_FILE)
}

fn get_point_file(device_id: &Uuid, group_id: &Uuid) -> PathBuf {
    get_dir()
        .join(device_id.to_string())
        .join(group_id.to_string())
        .join(POINT_FILE)
}

fn get_sink_file(device_id: &Uuid) -> PathBuf {
    get_dir().join(device_id.to_string()).join(SINK_FILE)
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

pub async fn read_coap_paths(device_id: &Uuid) -> Result<Vec<(Uuid, String)>, io::Error> {
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

pub async fn read_devices() -> Result<Vec<(Uuid, Vec<String>)>, io::Error> {
    let datas = super::read(get_device_file()).await?;
    let mut devices = vec![];
    for (id, raw_data) in datas {
        let data = raw_data.split(DELIMITER).map(|s| s.to_string()).collect();
        devices.push((id, data))
    }

    Ok(devices)
}

pub async unsafe fn update_device_conf(id: Uuid, conf: &Bytes) -> Result<(), io::Error> {
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
        if fields.len() != 3 {
            panic!("数据文件损坏");
        }
        if fields[0].parse::<Uuid>().expect("数据文件损坏") == id {
            new_line = format!(
                "{}{}{}{}{}",
                fields[0],
                DELIMITER,
                fields[1],
                DELIMITER,
                std::str::from_utf8_unchecked(conf)
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

pub async fn update_device_status(id: Uuid, status: Status) -> Result<(), io::Error> {
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
        if fields.len() != 3 {
            panic!("数据文件损坏");
        }
        if fields[0].parse::<Uuid>().expect("数据文件损坏") == id {
            new_line = format!(
                "{}{}{}{}{}",
                fields[0],
                DELIMITER,
                status.to_string(),
                DELIMITER,
                fields[2]
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

pub async fn insert_group(
    device_id: &Uuid,
    group_id: &Uuid,
    data: &String,
) -> Result<(), io::Error> {
    super::insert(get_group_file(device_id), group_id, data).await?;
    fs::create_dir(
        get_dir()
            .join(device_id.to_string())
            .join(group_id.to_string()),
    )
    .await?;
    super::create_file(
        get_dir()
            .join(device_id.to_string())
            .join(group_id.to_string())
            .join(POINT_FILE),
    )
    .await
}

pub async fn read_groups(device_id: &Uuid) -> Result<Vec<(Uuid, String)>, io::Error> {
    super::read(get_group_file(device_id)).await
}

pub async fn update_group(device_id: &Uuid, group_id: &Uuid, data: Bytes) -> Result<(), io::Error> {
    unsafe {
        super::update(
            get_group_file(device_id),
            group_id,
            std::str::from_utf8_unchecked(&data),
        )
        .await
    }
}

pub async fn delete_group(device_id: &Uuid, group_id: &Uuid) -> Result<(), io::Error> {
    super::delete(get_group_file(device_id), group_id).await
}

pub async fn insert_point(
    device_id: &Uuid,
    group_id: &Uuid,
    point_id: &Uuid,
    data: &String,
) -> Result<(), io::Error> {
    super::insert(get_point_file(device_id, group_id), point_id, data).await
}

pub async fn read_points(
    device_id: &Uuid,
    group_id: &Uuid,
) -> Result<Vec<(Uuid, String)>, io::Error> {
    super::read(get_point_file(device_id, group_id)).await
}

pub async fn update_point(
    device_id: &Uuid,
    group_id: &Uuid,
    point_id: &Uuid,
    data: &String,
) -> Result<(), io::Error> {
    super::update(get_point_file(device_id, group_id), point_id, data).await
}

pub async fn delete_points(
    device_id: &Uuid,
    group_id: &Uuid,
    point_ids: &Vec<Uuid>,
) -> Result<(), io::Error> {
    for point_id in point_ids {
        super::delete(get_point_file(device_id, group_id), point_id).await?;
    }
    Ok(())
}

pub async fn insert_sink(device_id: &Uuid, sink_id: &Uuid, data: &String) -> Result<(), io::Error> {
    super::insert(get_sink_file(device_id), sink_id, data).await
}

pub async fn read_sinks(device_id: &Uuid) -> Result<Vec<(Uuid, String)>, io::Error> {
    super::read(get_sink_file(device_id)).await
}

pub async fn update_sink(device_id: &Uuid, sink_id: &Uuid, data: &Bytes) -> Result<(), io::Error> {
    unsafe {
        super::update(
            get_sink_file(device_id),
            sink_id,
            std::str::from_utf8_unchecked(&data),
        )
        .await
    }
}

pub async fn delete_sink(device_id: &Uuid, sink_id: &Uuid) -> Result<(), io::Error> {
    super::delete(get_sink_file(device_id), sink_id).await
}
