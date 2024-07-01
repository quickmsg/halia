use std::{
    io,
    path::{Path, PathBuf},
};

use uuid::Uuid;

fn get_dir(device_id: &Uuid) -> PathBuf {
    Path::new(super::ROOT_DIR)
        .join(super::DEVICE_DIR)
        .join(device_id.to_string())
}

fn get_file(device_id: &Uuid) -> PathBuf {
    get_dir(device_id).join(super::DATA_FILE)
}

pub async fn insert(device_id: &Uuid, group_id: &Uuid, data: &String) -> Result<(), io::Error> {
    super::insert(get_file(device_id), group_id, data).await?;
    super::create_dir(get_dir(device_id).join(group_id.to_string())).await
}

pub async fn read(device_id: &Uuid) -> Result<Vec<(Uuid, String)>, io::Error> {
    super::read(get_file(device_id)).await
}

pub async fn update(device_id: &Uuid, group_id: &Uuid, data: &String) -> Result<(), io::Error> {
    super::update(get_file(device_id), group_id, data).await
}

pub async fn delete(device_id: &Uuid, group_id: &Uuid) -> Result<(), io::Error> {
    super::delete(get_file(device_id), group_id).await
}
