use std::{io, path::Path};

use serde::Deserialize;
use serde_json::Value;
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt},
};

static ROOT_DIR: &str = "./storage";
static FILE_NAME: &str = "data";

pub(crate) async fn insert_device(id: u64, data: String) -> Result<(), io::Error> {
    let device_path = Path::new(ROOT_DIR).join(FILE_NAME);
    let mut file = match fs::try_exists(&device_path).await? {
        true => OpenOptions::new().append(true).open(&device_path).await?,
        false => {
            OpenOptions::new()
                .read(true)
                .create_new(true)
                .write(true)
                .mode(0o466)
                .open(&device_path)
                .await?
        }
    };
    file.write(format!("{}-{}\n", id, data).as_bytes()).await?;
    Ok(())
}

pub(crate) async fn get_devices() -> Result<Vec<(u64, String)>, io::Error> {
    let device_path = Path::new(ROOT_DIR).join(FILE_NAME);
    let mut file = OpenOptions::new().read(true).open(device_path).await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    Ok(buf
        .split("\n")
        .map(|s| {
            let split_pos = s.find('-').expect("数据文件损坏");
            let (id, content) = s.split_at(split_pos);
            (
                id.parse::<u64>().expect("数据文件损坏"),
                content.to_string(),
            )
        })
        .collect())
}

pub(crate) async fn update_device(id: u64, data: String) -> Result<(), io::Error> {
    let device_path = Path::new(ROOT_DIR).join(FILE_NAME);
    let mut file = OpenOptions::new().write(true).open(device_path).await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let new_line = format!("{}-{}\n", id, data);
    let mut lines: Vec<&str> = buf.split("\n").collect();
    for line in lines.iter_mut() {
        let split_pos = line.find('-').expect("数据文件损坏");
        if line[..split_pos].parse::<u64>().expect("文件") == id {
            *line = new_line.as_str();
        }
    }

    let buf = lines.join("\n");
    file.write_all(buf.as_bytes()).await?;

    Ok(())
}

pub(crate) async fn delete_device(id: u64) -> Result<(), io::Error> {
    let device_path = Path::new(ROOT_DIR).join(FILE_NAME);
    let mut file = OpenOptions::new().write(true).open(device_path).await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let mut lines: Vec<&str> = buf.split("\n").collect();
    lines.retain(|line| {
        let split_pos = line.find('-').expect("数据文件损坏");
        line[..split_pos].parse::<u64>().expect("文件") != id
    });

    let buf = lines.join("\n");
    file.write_all(buf.as_bytes()).await?;

    Ok(())
}

pub(crate) async fn insert_group(
    device_id: u64,
    group_id: u64,
    data: &[u8],
) -> Result<(), io::Error> {
    Ok(())
}

pub(crate) async fn update_group(
    device_id: u64,
    group_id: u64,
    data: &[u8],
) -> Result<(), io::Error> {
    Ok(())
}

pub(crate) async fn delete_group(device_id: u64, group_id: u64) -> Result<(), io::Error> {
    Ok(())
}

pub(crate) async fn insert_points(
    device_id: u64,
    group_id: u64,
    datas: &[(u64, &[u8])],
) -> Result<(), io::Error> {
    Ok(())
}

pub(crate) async fn update_point(
    device_id: u64,
    group_id: u64,
    point_id: u64,
    data: &[u8],
) -> Result<(), io::Error> {
    Ok(())
}

pub(crate) async fn delete_point(
    device_id: u64,
    group_id: u64,
    points_ids: &[u64],
) -> Result<(), io::Error> {
    Ok(())
}

async fn insert(path: impl AsRef<Path>, record: &[u8]) -> Result<(), io::Error> {
    let mut file = match fs::try_exists(&path).await? {
        true => OpenOptions::new().append(true).open(&path).await?,
        false => File::create(&path).await?,
    };

    file.write_all(record).await?;
    file.flush().await
}

async fn create_dir(path: impl AsRef<Path>) -> Result<(), io::Error> {
    match fs::try_exists(&path).await? {
        true => Ok(()),
        false => fs::create_dir_all(&path).await,
    }
}

async fn read_file(path: impl AsRef<Path>) -> Result<(), io::Error> {
    todo!()
}

async fn update(path: impl AsRef<Path>, record: &[u8], id: u64) -> Result<(), io::Error> {
    todo!()
}

// async fn delete(path: impl AsRef<Path>, id: u64) -> Result<(), io::Error> {
//     let mut file = File::open(&path).await?;

//     let mut contents = String::new();
//     file.read_to_string(&mut contents).await?;

//     let mut lines: Vec<&str> = contents.lines().collect();
//     lines.retain(|line| match serde_json::from_str::<Record>(line) {
//         Ok(record) => record.id != id,
//         Err(_) => {
//             error!("文件已损坏");
//             true
//         }
//     });

//     let new_contents = lines.join("\n");

//     let mut file = File::create(&path).await?;
//     file.write_all(new_contents.as_bytes()).await?;
//     file.flush().await?;

//     Ok(())
// }
