use std::{io, path::Path};

use uuid::Uuid;

use super::{DATA_FILE, ROOT_DIR};

pub async fn insert(
    device_id: Uuid,
    group_id: Uuid,
    datas: &Vec<(Uuid, String)>,
) -> Result<(), io::Error> {
    super::insert(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(group_id.to_string()),
        datas,
        false,
    )
    .await
}

pub async fn read(device_id: Uuid, group_id: Uuid) -> Result<Vec<(Uuid, String)>, io::Error> {
    super::read(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(group_id.to_string())
            .join(DATA_FILE),
    )
    .await
}

pub async fn update(
    device_id: Uuid,
    group_id: Uuid,
    point_id: Uuid,
    data: String,
) -> Result<(), io::Error> {
    super::update(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(group_id.to_string())
            .join(DATA_FILE),
        point_id,
        data,
    )
    .await
}

pub async fn delete(
    device_id: Uuid,
    group_id: Uuid,
    point_ids: &Vec<Uuid>,
) -> Result<(), io::Error> {
    super::delete(
        Path::new(ROOT_DIR)
            .join(device_id.to_string())
            .join(group_id.to_string())
            .join(DATA_FILE),
        point_ids,
    )
    .await
}
