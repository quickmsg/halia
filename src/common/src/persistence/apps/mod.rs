use std::{
    io,
    path::{Path, PathBuf},
};

use tokio::fs;
use uuid::Uuid;

use super::Status;

pub mod http_client;
pub mod mqtt_client;

static APP_DIR: &str = "apps";
static APP_FILE: &str = "apps";

fn get_app_dir() -> PathBuf {
    Path::new(super::ROOT_DIR).join(APP_DIR)
}

fn get_app_file_path() -> PathBuf {
    get_app_dir().join(APP_FILE)
}

pub async fn init() -> Result<(), io::Error> {
    fs::create_dir_all(get_app_dir()).await?;
    super::create_file(get_app_file_path()).await
}

pub async fn read_apps() -> Result<Vec<String>, io::Error> {
    super::read(get_app_file_path()).await
}

pub async fn update_app_conf(app_id: &Uuid, data: String) -> Result<(), io::Error> {
    super::update_segment(get_app_file_path(), app_id, 3, &data).await
}

pub async fn update_app_status(app_id: &Uuid, status: Status) -> Result<(), io::Error> {
    super::update_segment(get_app_file_path(), app_id, 2, &status.to_string()).await
}

pub async fn delete_app(app_id: &Uuid) -> Result<(), io::Error> {
    super::delete(get_app_file_path(), app_id).await?;
    fs::remove_dir_all(get_app_dir().join(app_id.to_string())).await
}
