use std::{io, path::PathBuf};

use tokio::fs;
use types::devices::DeviceType;
use uuid::Uuid;

use crate::persistence;

use super::{get_device_dir, get_device_file_path, Status, DELIMITER};

static GROUP_FILE: &str = "groups";
static API_FILE: &str = "apis";
static SINK_FILE: &str = "sinks";

fn get_group_file_path(device_id: &Uuid) -> PathBuf {
    get_device_dir()
        .join(device_id.to_string())
        .join(GROUP_FILE)
}

fn get_sink_file_path(device_id: &Uuid) -> PathBuf {
    get_device_dir().join(device_id.to_string()).join(SINK_FILE)
}

fn get_api_file_path(device_id: &Uuid) -> PathBuf {
    get_device_dir().join(device_id.to_string()).join(API_FILE)
}

pub async fn create(device_id: &Uuid, data: String) -> Result<(), io::Error> {
    crate::persistence::create(
        get_device_file_path(),
        device_id,
        &format!(
            "{}{}{}{}{}",
            DeviceType::Coap,
            DELIMITER,
            Status::Stopped,
            DELIMITER,
            data,
        ),
    )
    .await?;

    fs::create_dir_all(get_device_dir().join(device_id.to_string())).await?;
    crate::persistence::create_file(get_group_file_path(device_id)).await?;
    crate::persistence::create_file(get_sink_file_path(device_id)).await
}

pub async fn create_api(device_id: &Uuid, api_id: &Uuid, data: String) -> Result<(), io::Error> {
    persistence::create(get_api_file_path(device_id), api_id, &data).await
}

pub async fn read_apis(device_id: &Uuid) -> Result<Vec<String>, io::Error> {
    persistence::read(get_api_file_path(device_id)).await
}

pub async fn update_api(device_id: &Uuid, api_id: &Uuid, data: String) -> Result<(), io::Error> {
    persistence::update(get_api_file_path(device_id), api_id, &data).await
}

pub async fn delete_api(device_id: &Uuid, api_id: &Uuid) -> Result<(), io::Error> {
    persistence::delete(get_api_file_path(device_id), api_id).await
}

pub async fn create_sink(device_id: &Uuid, sink_id: &Uuid, data: String) -> Result<(), io::Error> {
    persistence::create(get_sink_file_path(device_id), sink_id, &data).await?;
    fs::create_dir(
        get_device_dir()
            .join(device_id.to_string())
            .join(sink_id.to_string()),
    )
    .await
}

pub async fn read_sinks(device_id: &Uuid) -> Result<Vec<String>, io::Error> {
    persistence::read(get_sink_file_path(device_id)).await
}

pub async fn update_sink(device_id: &Uuid, sink_id: &Uuid, data: String) -> Result<(), io::Error> {
    persistence::update(get_sink_file_path(device_id), sink_id, &data).await
}

pub async fn delete_sink(device_id: &Uuid, sink_id: &Uuid) -> Result<(), io::Error> {
    persistence::delete(get_sink_file_path(device_id), sink_id).await?;
    fs::remove_dir_all(
        get_device_dir()
            .join(device_id.to_string())
            .join(sink_id.to_string()),
    )
    .await
}
