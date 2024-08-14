use std::{fs, io, path::PathBuf};

use uuid::Uuid;

use crate::persistence::{self, create_file, DELIMITER};

use super::{get_app_dir, get_app_file_path};

static SOURCE_DIR: &str = "sources";
static SINK_DIR: &str = "sinks";

fn get_source_file_path(app_id: &Uuid) -> PathBuf {
    get_app_dir().join(app_id.to_string()).join(SOURCE_DIR)
}

fn get_sink_file_path(app_id: &Uuid) -> PathBuf {
    get_app_dir().join(app_id.to_string()).join(SINK_DIR)
}

pub async fn create(app_id: &Uuid, r#type: &str, data: String) -> Result<(), io::Error> {
    persistence::create(
        get_app_file_path(),
        app_id,
        &format!("{}{}{}", r#type, DELIMITER, data),
    )
    .await?;
    fs::create_dir(get_app_dir().join(app_id.to_string()))?;
    create_file(get_sink_file_path(app_id)).await
}

pub async fn delete(app_id: &Uuid) -> Result<(), io::Error> {
    persistence::delete(get_app_file_path(), app_id).await
}

pub async fn create_source(app_id: &Uuid, source_id: &Uuid, data: String) -> Result<(), io::Error> {
    persistence::create(get_source_file_path(app_id), source_id, &data).await
}

pub async fn read_sources(app_id: &Uuid) -> Result<Vec<String>, io::Error> {
    persistence::read(get_source_file_path(app_id)).await
}

pub async fn update_source(app_id: &Uuid, source_id: &Uuid, data: String) -> Result<(), io::Error> {
    persistence::update(get_source_file_path(app_id), source_id, &data).await
}

pub async fn delete_source(app_id: &Uuid, source_id: &Uuid) -> Result<(), io::Error> {
    persistence::delete(get_source_file_path(app_id), source_id).await
}

pub async fn create_sink(app_id: &Uuid, sink_id: &Uuid, data: String) -> Result<(), io::Error> {
    persistence::create(get_sink_file_path(app_id), sink_id, &data).await
}

pub async fn read_sinks(app_id: &Uuid) -> Result<Vec<String>, io::Error> {
    persistence::read(get_sink_file_path(app_id)).await
}

pub async fn update_sink(app_id: &Uuid, sink_id: &Uuid, data: String) -> Result<(), io::Error> {
    persistence::update(get_sink_file_path(app_id), sink_id, &data).await
}

pub async fn delete_sink(app_id: &Uuid, sink_id: &Uuid) -> Result<(), io::Error> {
    persistence::delete(get_sink_file_path(app_id), sink_id).await
}