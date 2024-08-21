use std::{
    io,
    path::{Path, PathBuf},
};

use tokio::fs;
use uuid::Uuid;

use super::{update_segment, Status, DELIMITER};

static DEVICE_FILE: &str = "devices";
static DEVICE_DIR: &str = "devices";

pub mod coap;
pub mod modbus;
pub mod opcua;

pub(crate) fn get_device_dir() -> PathBuf {
    Path::new(super::ROOT_DIR).join(DEVICE_DIR)
}

pub(crate) fn get_device_file_path() -> PathBuf {
    get_device_dir().join(DEVICE_FILE)
}

pub async fn init() -> Result<(), io::Error> {
    fs::create_dir_all(get_device_dir()).await?;
    super::create_file(get_device_dir().join(DEVICE_FILE)).await
}

pub async fn read_devices() -> Result<Vec<String>, io::Error> {
    super::read(get_device_file_path()).await
}

pub async fn update_device_conf(id: &Uuid, conf: String) -> Result<(), io::Error> {
    update_segment(get_device_file_path(), id, 3, &conf).await
}

pub async fn update_device_status(id: &Uuid, status: Status) -> Result<(), io::Error> {
    update_segment(get_device_file_path(), id, 2, &status.to_string()).await
}

pub async fn delete_device(id: &Uuid) -> Result<(), io::Error> {
    super::delete(get_device_file_path(), id).await?;
    fs::remove_dir_all(get_device_dir().join(id.to_string())).await
}

