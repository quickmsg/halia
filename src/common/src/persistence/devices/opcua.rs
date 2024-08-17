use std::{io, path::PathBuf};

use tokio::fs;
use uuid::Uuid;

use crate::persistence;

use super::{get_device_dir, get_device_file_path, Status, DELIMITER};

static SINK_FILE: &str = "sinks";
static VARIABLE_FILE: &str = "variables";
static GROUP_FILE: &str = "groups";
static SUBSCRIPTION_FILE: &str = "subscriptions";

fn get_group_file_path(device_id: &Uuid) -> PathBuf {
    get_device_dir()
        .join(device_id.to_string())
        .join(GROUP_FILE)
}

fn get_group_variable_file_path(device_id: &Uuid, group_id: &Uuid) -> PathBuf {
    get_device_dir()
        .join(device_id.to_string())
        .join(group_id.to_string())
        .join(VARIABLE_FILE)
}

fn get_subscription_file_path(device_id: &Uuid) -> PathBuf {
    get_device_dir()
        .join(device_id.to_string())
        .join(SUBSCRIPTION_FILE)
}

fn get_sink_file_path(device_id: &Uuid) -> PathBuf {
    get_device_dir().join(device_id.to_string()).join(SINK_FILE)
}

pub async fn create(device_id: &Uuid, r#type: &'static str, data: String) -> Result<(), io::Error> {
    crate::persistence::create(
        get_device_file_path(),
        device_id,
        &format!(
            "{}{}{}{}{}",
            r#type,
            DELIMITER,
            Status::Stopped,
            DELIMITER,
            data,
        ),
    )
    .await?;

    fs::create_dir(get_device_dir().join(device_id.to_string())).await?;
    persistence::create_file(get_group_file_path(device_id)).await?;
    persistence::create_file(get_subscription_file_path(device_id)).await?;
    persistence::create_file(get_sink_file_path(device_id)).await
}

pub async fn create_group(
    device_id: &Uuid,
    group_id: &Uuid,
    data: String,
) -> Result<(), io::Error> {
    fs::create_dir(
        get_device_dir()
            .join(device_id.to_string())
            .join(group_id.to_string()),
    )
    .await?;
    crate::persistence::create_file(get_group_variable_file_path(device_id, group_id)).await?;
    crate::persistence::create(get_group_file_path(device_id), group_id, &data).await
}

pub async fn read_groups(device_id: &Uuid) -> Result<Vec<String>, io::Error> {
    persistence::read(get_group_file_path(device_id)).await
}

pub async fn update_group(
    device_id: &Uuid,
    group_id: &Uuid,
    data: String,
) -> Result<(), io::Error> {
    crate::persistence::update(get_group_file_path(device_id), group_id, &data).await
}

pub async fn delete_group(device_id: &Uuid, group_id: &Uuid) -> Result<(), io::Error> {
    crate::persistence::delete(get_group_file_path(device_id), group_id).await?;
    fs::remove_dir(
        get_device_dir()
            .join(device_id.to_string())
            .join(group_id.to_string()),
    )
    .await
}

pub async fn create_group_variable(
    device_id: &Uuid,
    group_id: &Uuid,
    variable_id: &Uuid,
    data: String,
) -> Result<(), io::Error> {
    persistence::create(
        get_group_variable_file_path(device_id, group_id),
        variable_id,
        &data,
    )
    .await
}

pub async fn read_group_variables(
    device_id: &Uuid,
    group_id: &Uuid,
) -> Result<Vec<String>, io::Error> {
    persistence::read(get_group_variable_file_path(device_id, group_id)).await
}

pub async fn update_group_variable(
    device_id: &Uuid,
    group_id: &Uuid,
    variable_id: &Uuid,
    data: String,
) -> Result<(), io::Error> {
    persistence::update(
        get_group_variable_file_path(device_id, group_id),
        variable_id,
        &data,
    )
    .await
}

pub async fn delete_group_variable(
    device_id: &Uuid,
    group_id: &Uuid,
    variable_id: &Uuid,
) -> Result<(), io::Error> {
    persistence::delete(
        get_group_variable_file_path(device_id, group_id),
        variable_id,
    )
    .await
}

pub async fn create_subscription(
    device_id: &Uuid,
    subscription_id: &Uuid,
    data: String,
) -> Result<(), io::Error> {
    persistence::create(
        get_subscription_file_path(device_id),
        subscription_id,
        &data,
    )
    .await
}

pub async fn read_subscriptions(device_id: &Uuid) -> Result<Vec<String>, io::Error> {
    persistence::read(get_subscription_file_path(device_id)).await
}

pub async fn update_subscription(
    device_id: &Uuid,
    subscription_id: &Uuid,
    data: String,
) -> Result<(), io::Error> {
    persistence::update(
        get_subscription_file_path(device_id),
        subscription_id,
        &data,
    )
    .await
}

pub async fn delete_subscription(
    device_id: &Uuid,
    subscription_id: &Uuid,
) -> Result<(), io::Error> {
    persistence::delete(get_subscription_file_path(device_id), subscription_id).await
}

pub async fn create_sink(device_id: &Uuid, sink_id: &Uuid, data: String) -> Result<(), io::Error> {
    persistence::create(get_sink_file_path(device_id), sink_id, &data).await
}

pub async fn read_sinks(device_id: &Uuid) -> Result<Vec<String>, io::Error> {
    persistence::read(get_sink_file_path(device_id)).await
}

pub async fn update_sink(device_id: &Uuid, sink_id: &Uuid, data: String) -> Result<(), io::Error> {
    persistence::update(get_sink_file_path(device_id), sink_id, &data).await
}

pub async fn delete_sink(device_id: &Uuid, sink_id: &Uuid) -> Result<(), io::Error> {
    persistence::delete(get_sink_file_path(device_id), sink_id).await
}
