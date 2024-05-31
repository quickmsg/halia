use std::{fs::Permissions, io, os::unix::fs::PermissionsExt, path::Path};

use tokio::{
    fs::{self, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

static ROOT_DIR: &str = "./storage";
static FILE_NAME: &str = "data";
static DELIMITER: char = '-';

pub(crate) async fn insert_device(id: u64, data: String) -> Result<(), io::Error> {
    insert(Path::new(ROOT_DIR).join(FILE_NAME), id, data).await?;
    fs::create_dir(Path::new(ROOT_DIR).join(id.to_string())).await
}

pub(crate) async fn read_devices() -> Result<Vec<(u64, String)>, io::Error> {
    read(Path::new(ROOT_DIR).join(FILE_NAME)).await
}

pub(crate) async fn update_device(id: u64, data: String) -> Result<(), io::Error> {
    update(Path::new(ROOT_DIR).join(FILE_NAME), id, data).await
}

pub(crate) async fn delete_device(id: u64) -> Result<(), io::Error> {
    delete(Path::new(ROOT_DIR).join(FILE_NAME), id).await?;
    fs::remove_dir(Path::new(ROOT_DIR).join(id.to_string())).await
}

pub(crate) async fn insert_group(
    device_id: u64,
    group_id: u64,
    data: String,
) -> Result<(), io::Error> {
    insert(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(FILE_NAME),
        group_id,
        data,
    )
    .await?;

    fs::create_dir(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(group_id.to_string()),
    )
    .await
}

pub(crate) async fn read_groups(device_id: u64) -> Result<Vec<(u64, String)>, io::Error> {
    read(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(FILE_NAME),
    )
    .await
}

pub(crate) async fn update_group(
    device_id: u64,
    group_id: u64,
    data: String,
) -> Result<(), io::Error> {
    update(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(FILE_NAME),
        group_id,
        data,
    )
    .await
}

pub(crate) async fn delete_group(device_id: u64, group_id: u64) -> Result<(), io::Error> {
    delete(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(FILE_NAME),
        group_id,
    )
    .await
}

pub(crate) async fn insert_points(
    device_id: u64,
    group_id: u64,
    datas: &[(u64, String)],
) -> Result<(), io::Error> {
    let path = Path::new(ROOT_DIR)
        .join(device_id.to_string())
        .join(group_id.to_string())
        .join(FILE_NAME);
    let mut file = match fs::try_exists(&path).await? {
        true => OpenOptions::new().append(true).open(&path).await?,
        false => {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&path)
                .await?;
            file.set_permissions(Permissions::from_mode(0o666)).await?;
            file
        }
    };

    for (id, data) in datas {
        file.write(format!("{}{}{}\n", id, DELIMITER, data).as_bytes())
            .await?;
    }
    file.flush().await
}

pub(crate) async fn read_points(
    device_id: u64,
    group_id: u64,
) -> Result<Vec<(u64, String)>, io::Error> {
    read(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(group_id.to_string())
            .join(FILE_NAME),
    )
    .await
}

pub(crate) async fn update_point(
    device_id: u64,
    group_id: u64,
    point_id: u64,
    data: String,
) -> Result<(), io::Error> {
    update(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(group_id.to_string())
            .join(FILE_NAME),
        point_id,
        data,
    )
    .await
}

pub(crate) async fn delete_point(
    device_id: u64,
    group_id: u64,
    points_ids: Vec<u64>,
) -> Result<(), io::Error> {
    let path = Path::new(ROOT_DIR)
        .join(device_id.to_string())
        .join(group_id.to_string())
        .join(FILE_NAME);
    let mut file = OpenOptions::new().read(true).open(&path).await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let mut lines: Vec<&str> = buf.split("\n").collect();
    lines.retain(|line| match line.find(DELIMITER) {
        Some(pos) => points_ids.contains(&line[..pos].parse::<u64>().expect("文件")),
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

async fn insert(path: impl AsRef<Path>, id: u64, data: String) -> Result<(), io::Error> {
    let mut file = match fs::try_exists(&path).await? {
        true => OpenOptions::new().append(true).open(&path).await?,
        false => {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .open(&path)
                .await?;
            file.set_permissions(Permissions::from_mode(0o666)).await?;
            file
        }
    };
    file.write(format!("{}{}{}\n", id, DELIMITER, data).as_bytes())
        .await?;
    file.flush().await
}

async fn read(path: impl AsRef<Path>) -> Result<Vec<(u64, String)>, io::Error> {
    let mut file = OpenOptions::new().read(true).open(path).await?;
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

async fn update(path: impl AsRef<Path>, id: u64, data: String) -> Result<(), io::Error> {
    let mut file = OpenOptions::new().write(true).open(path).await?;
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

async fn delete(path: impl AsRef<Path>, id: u64) -> Result<(), io::Error> {
    let mut file = OpenOptions::new().read(true).open(&path).await?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).await?;

    let mut lines: Vec<&str> = buf.split("\n").collect();
    lines.retain(|line| match line.find(DELIMITER) {
        Some(pos) => line[..pos].parse::<u64>().expect("文件") != id,
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
