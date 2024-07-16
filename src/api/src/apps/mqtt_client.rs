use apps::mqtt_client::manager::GLOBAL_MQTT_CLIENT_MANAGER;
use axum::extract::{Path, Query};
use uuid::Uuid;

use crate::{AppResp, Pagination};

pub(crate) async fn create(data: String) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_MANAGER.create(None, data).await {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn update(Path(app_id): Path<Uuid>, data: String) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_MANAGER.update(app_id, data).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete(Path(app_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_MANAGER.delete(app_id).await {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}

pub(crate) async fn create_source(Path(app_id): Path<Uuid>, data: String) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_MANAGER
        .create_source(app_id, None, data)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

// pub(crate) async fn search_sources(
//     Path(app_id): Path<Uuid>,
//     pagination: Query<Pagination>,
// ) -> AppResp<SearchSourceResp> {
//     match GLOBAL_MQTT_CLIENT_MANAGER
//         .search_sources(app_id, pagination.p, pagination.s)
//         .await
//     {
//         Ok(data) => AppResp::with_data(data),
//         Err(e) => e.into(),
//     }
// }

pub(crate) async fn update_source(
    Path((app_id, source_id)): Path<(Uuid, Uuid)>,
    data: String,
) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_MANAGER
        .update_source(app_id, source_id, data)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_source(Path((app_id, source_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_MANAGER
        .delete_source(app_id, source_id)
        .await
    {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}

pub(crate) async fn create_sink(Path(app_id): Path<Uuid>, data: String) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_MANAGER
        .create_sink(app_id, None, data)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
}

// pub(crate) async fn search_sinks(
//     Path(app_id): Path<Uuid>,
//     pagination: Query<Pagination>,
// ) -> AppResp<SearchSinkResp> {
//     match GLOBAL_MQTT_CLIENT_MANAGER
//         .search_sinks(app_id, pagination.p, pagination.s)
//         .await
//     {
//         Ok(data) => AppResp::with_data(data),
//         Err(e) => e.into(),
//     }
// }

pub(crate) async fn update_sink(
    Path((app_id, sink_id)): Path<(Uuid, Uuid)>,
    data: String,
) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_MANAGER
        .update_sink(app_id, sink_id, data)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

pub(crate) async fn delete_sink(Path((app_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_MQTT_CLIENT_MANAGER
        .delete_sink(app_id, sink_id)
        .await
    {
        Ok(_) => todo!(),
        Err(_) => todo!(),
    }
}
