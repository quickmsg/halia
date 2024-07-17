use std::{
    io,
    path::{Path, PathBuf},
};

use tokio::fs;
use uuid::Uuid;

pub mod mqtt_client;

static APP_DIR: &str = "apps";

fn get_app_dir() -> PathBuf {
    Path::new(super::ROOT_DIR).join(APP_DIR)
}

fn get_app_file_path() -> PathBuf {
    get_app_dir().join(super::DATA_FILE)
}

pub async fn init() -> Result<(), io::Error> {
    fs::create_dir_all(get_app_dir()).await?;
    super::create_file(get_app_file_path()).await
}

pub async fn read_apps() -> Result<Vec<String>, io::Error> {
    super::read(get_app_file_path()).await
}

pub async fn update_app(app_id: &Uuid, data: String) -> Result<(), io::Error> {
    super::update(get_app_file_path(), app_id, &data).await
}
