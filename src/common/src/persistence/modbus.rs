use std::{io, path::PathBuf};

use tokio::fs;
use uuid::Uuid;

use super::{
    device::{get_device_dir, get_device_file_path},
    Status, DELIMITER,
};

static GROUP_FILE: &str = "groups";
static SINK_FILE: &str = "sinks";
static POINT_FILE: &str = "points";

fn get_group_file_path(device_id: &Uuid) -> PathBuf {
    get_device_dir()
        .join(device_id.to_string())
        .join(GROUP_FILE)
}

fn get_sink_file_path(device_id: &Uuid) -> PathBuf {
    get_device_dir().join(device_id.to_string()).join(SINK_FILE)
}

fn get_group_point_file_path(device_id: &Uuid, group_id: &Uuid) -> PathBuf {
    get_device_dir()
        .join(device_id.to_string())
        .join(group_id.to_string())
        .join(POINT_FILE)
}

pub async fn create(device_id: &Uuid, data: &String) -> Result<(), io::Error> {
    super::create(
        get_device_file_path(),
        device_id,
        &format!("{}{}{}", Status::Stopped, DELIMITER, data),
    )
    .await?;

    fs::create_dir_all(get_device_dir().join(device_id.to_string())).await?;
    super::create_file(get_group_file_path(device_id)).await?;
    super::create_file(get_sink_file_path(device_id)).await
}

pub async fn create_group(
    device_id: &Uuid,
    group_id: &Uuid,
    data: &String,
) -> Result<(), io::Error> {
    super::create(get_group_file_path(device_id), group_id, data).await?;
    fs::create_dir(
        get_device_dir()
            .join(device_id.to_string())
            .join(group_id.to_string()),
    )
    .await?;
    super::create_file(get_group_point_file_path(device_id, group_id)).await
}

pub async fn read_groups(device_id: &Uuid) -> Result<Vec<(Uuid, String)>, io::Error> {
    super::read(get_group_file_path(device_id)).await
}

pub async fn update_group() {}

pub async fn remove_group() {}

pub async fn create_group_point(
    device_id: &Uuid,
    group_id: &Uuid,
    point_id: &Uuid,
    data: &String,
) -> Result<(), io::Error> {
    super::create(
        get_group_point_file_path(device_id, group_id),
        point_id,
        data,
    )
    .await
}

pub async fn read_group_points() {}

pub async fn update_group_point() {}

pub async fn delete_group_points() {}

pub async fn create_sink(device_id: &Uuid, sink_id: &Uuid, data: &String) {}
