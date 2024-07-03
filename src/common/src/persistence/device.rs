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

use super::{Status, DELIMITER};

fn get_dir() -> PathBuf {
    Path::new(super::ROOT_DIR).join(super::DEVICE_DIR)
}

fn get_file() -> PathBuf {
    get_dir().join(super::DATA_FILE)
}

pub async fn init() -> Result<(), io::Error> {
    fs::create_dir_all(get_dir()).await
}

pub async unsafe fn insert(id: &Uuid, data: &Bytes) -> Result<(), io::Error> {
    let data = format!("{}{}{}", 0, DELIMITER, std::str::from_utf8_unchecked(data));
    super::insert(get_file(), id, &data).await?;
    fs::create_dir_all(get_dir().join(id.to_string())).await
}

pub async fn read() -> Result<Vec<(Uuid, Status, String)>, io::Error> {
    let datas = super::read(get_file()).await?;
    let mut devices = vec![];
    for (id, data) in datas {
        let pos = data.find(DELIMITER).expect("数据文件损坏");
        let status = &data[..pos];
        let device = &data[pos + 1..];
        let status = status
            .parse::<u8>()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "数据文件损坏"))?;
        let status = match status {
            0 => Status::Stopped,
            1 => Status::Runing,
            _ => unreachable!(),
        };
        devices.push((id, status, device.to_string()))
    }

    Ok(devices)
}

pub async unsafe fn update_conf(id: Uuid, conf: &Bytes) -> Result<(), io::Error> {
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

pub async fn update_status(id: Uuid, status: Status) -> Result<(), io::Error> {
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

pub async fn delete(id: &Uuid) -> Result<(), io::Error> {
    super::delete(get_file(), id).await?;
    fs::remove_dir_all(get_dir().join(id.to_string())).await
}
