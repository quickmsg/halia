use apps::GLOBAL_APP_MANAGER;
use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use types::{
    apps::{CreateUpdateAppReq, QueryParams, SearchAppsResp, Summary},
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp,
};
use uuid::Uuid;

use crate::{AppResult, AppSuccess};

pub fn routes() -> Router {
    Router::new()
        .route("/summary", get(get_apps_summary))
        .route("/", post(create_app))
        .route("/", get(search_apps))
        .route("/:app_id", put(update_app))
        .route("/:app_id/start", put(start_app))
        .route("/:app_id/stop", put(stop_app))
        .route("/:app_id", routing::delete(delete_app))
        .nest(
            "/:app_id",
            Router::new()
                .nest(
                    "/source",
                    Router::new()
                        .route("/", post(create_source))
                        .route("/", get(search_sources))
                        .route("/:source_id", put(update_source))
                        .route("/:source_id", routing::delete(delete_source)),
                )
                .nest(
                    "/sink",
                    Router::new()
                        .route("/", post(create_sink))
                        .route("/", get(search_sinks))
                        .route("/:sink_id", put(update_sink))
                        .route("/:sink_id", routing::delete(delete_sink)),
                ),
        )
}

async fn get_apps_summary() -> AppSuccess<Summary> {
    AppSuccess::data(GLOBAL_APP_MANAGER.get_summary().await)
}

async fn create_app(Json(req): Json<CreateUpdateAppReq>) -> AppResult<AppSuccess<()>> {
    let app_id = Uuid::new_v4();
    GLOBAL_APP_MANAGER.create_app(app_id, req, false).await?;
    Ok(AppSuccess::empty())
}

async fn search_apps(
    Query(pagination): Query<Pagination>,
    Query(query_params): Query<QueryParams>,
) -> AppSuccess<SearchAppsResp> {
    AppSuccess::data(
        GLOBAL_APP_MANAGER
            .search_apps(pagination, query_params)
            .await,
    )
}

// async fn create_todo(mut multipart: Multipart) -> AppResult<AppSuccess<()>> {
//     let mut req: Option<CreateUpdateMqttClientReq> = None;

//     let mut ca: Option<Bytes> = None;
//     let mut client_cert: Option<Bytes> = None;
//     let mut client_key: Option<Bytes> = None;
//     while let Some(field) = multipart.next_field().await.unwrap() {
//         match field.name() {
//             Some(name) => match name {
//                 "req" => match field.bytes().await {
//                     Ok(data) => match serde_json::from_slice::<CreateUpdateMqttClientReq>(&data) {
//                         Ok(json_req) => req = Some(json_req),
//                         Err(e) => {
//                             return Err(AppError::new(format!("序列化错误:{}", e.to_string())))
//                         }
//                     },
//                     Err(e) => return Err(AppError::new(e.to_string())),
//                 },
//                 "ca" => match field.bytes().await {
//                     Ok(data) => ca = Some(data),
//                     Err(e) => return Err(AppError::new(e.to_string())),
//                 },

//                 "client_cert" => match field.bytes().await {
//                     Ok(data) => client_cert = Some(data),
//                     Err(e) => return Err(AppError::new(e.to_string())),
//                 },

//                 "client_key" => match field.bytes().await {
//                     Ok(data) => client_key = Some(data),
//                     Err(e) => return Err(AppError::new(e.to_string())),
//                 },

//                 _ => {
//                     return Err(AppError {
//                         code: 1,
//                         data: format!("多余的字段:{}", name).to_owned(),
//                     })
//                 }
//             },
//             None => return Err(AppError::new("缺少字段名".to_owned())),
//         }
//     }

//     match req {
//         Some(req) => match GLOBAL_MQTT_CLIENT_MANAGER.create(None, req).await {
//             Ok(_) => Ok(AppSuccess::empty()),
//             Err(e) => Err(e.into()),
//         },
//         None => Err(AppError::new("缺少配置".to_owned())),
//     }
// }

async fn update_app(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateAppReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.update_app(app_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn start_app(Path(app_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.start_app(app_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop_app(Path(app_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.stop_app(app_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete_app(Path(app_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.delete_app(app_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_source(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.create_source(app_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_sources(
    Path(app_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let data = GLOBAL_APP_MANAGER
        .search_sources(app_id, pagination, query)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_source(
    Path((app_id, source_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER
        .update_source(app_id, source_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_source(Path((app_id, source_id)): Path<(Uuid, Uuid)>) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.delete_source(app_id, source_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    Path(app_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.create_sink(app_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    Path(app_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
    Query(query): Query<QueryParams>,
) -> AppResult<AppSuccess<SearchSourcesOrSinksResp>> {
    let data = GLOBAL_APP_MANAGER
        .search_sinks(app_id, pagination, query)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_sink(
    Path((app_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSourceOrSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.update_sink(app_id, sink_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(Path((app_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResult<AppSuccess<()>> {
    GLOBAL_APP_MANAGER.delete_sink(app_id, sink_id).await?;
    Ok(AppSuccess::empty())
}
