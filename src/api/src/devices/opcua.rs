use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use devices::opcua::manager::GLOBAL_OPCUA_MANAGER;
use types::{
    devices::{
        opcua::{
            CreateUpdateEventReq, CreateUpdateGroupReq, CreateUpdateGroupVariableReq,
            CreateUpdateOpcuaReq, CreateUpdateSinkReq, CreateUpdateSubscriptionReq,
            SearchEventsResp, SearchGroupVariablesResp, SearchGroupsResp, SearchSinksResp,
            SearchSubscriptionsResp,
        },
        SearchDevicesItemResp,
    },
    Pagination,
};
use uuid::Uuid;

use crate::{AppResult, AppSuccess};

pub(crate) fn opcua_routes() -> Router {
    Router::new()
        .route("/", post(create))
        .route("/:device_id", get(read))
        .route("/:device_id", put(update))
        .route("/:device_id/start", put(start))
        .route("/:device_id/stop", put(stop))
        .route("/:device_id", routing::delete(delete))
        .nest(
            "/:device_id/group",
            Router::new()
                .route("/", post(create_group))
                .route("/", get(search_groups))
                .route("/:group_id", put(update_group))
                .route("/:group_id", routing::delete(delete_group))
                .nest(
                    "/:group_id/variable",
                    Router::new()
                        .route("/", post(create_group_variable))
                        .route("/", get(search_group_variables))
                        .route("/:variable_id", put(update_group_variable))
                        // .route("/:variable_id/value", put(write_group_variable_value))
                        .route("/:variable_id", routing::delete(delete_group_variable)),
                ),
        )
        .nest(
            "/:device_id/subscription",
            Router::new()
                .route("/", post(create_subscription))
                .route("/", get(search_subscriptions))
                .route("/:subscription_id", put(update_subscription))
                .route("/:subscription_id", routing::delete(delete_subscription)),
        )
        .nest(
            "/:device_id/event",
            Router::new()
                .route("/", post(create_event))
                .route("/", get(search_events))
                .route("/:event_id", put(update_event))
                .route("/:event_id", routing::delete(delete_event)),
        )
        .nest(
            "/:device_id/sink",
            Router::new()
                .route("/", post(create_sink))
                .route("/", get(search_sinks))
                .route("/:sink_id", put(update_sink))
                .route("/:sink_id", routing::delete(delete_sink)),
        )
}

async fn create(Json(req): Json<CreateUpdateOpcuaReq>) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER.create(None, req).await?;
    Ok(AppSuccess::empty())
}

async fn read(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<SearchDevicesItemResp>> {
    let data = GLOBAL_OPCUA_MANAGER.search(&device_id)?;
    Ok(AppSuccess::data(data))
}

async fn update(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateOpcuaReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER.update(device_id, req).await?;
    Ok(AppSuccess::empty())
}

async fn start(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER.start(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn stop(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER.stop(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn delete(Path(device_id): Path<Uuid>) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER.delete(device_id).await?;
    Ok(AppSuccess::empty())
}

async fn create_group(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateGroupReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .create_group(device_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_groups(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchGroupsResp>> {
    let data = GLOBAL_OPCUA_MANAGER
        .search_groups(device_id, pagination)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_group(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .update_group(device_id, group_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_group(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .delete_group(device_id, group_id)
        .await?;
    Ok(AppSuccess::empty())
}

async fn create_group_variable(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupVariableReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .create_group_variable(device_id, group_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_group_variables(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchGroupVariablesResp>> {
    let data = GLOBAL_OPCUA_MANAGER
        .search_group_variables(device_id, group_id, pagination)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_group_variable(
    Path((device_id, group_id, variable_id)): Path<(Uuid, Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupVariableReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .update_group_variable(device_id, group_id, variable_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

// async fn write_group_point_value(
//     Path((device_id, group_id, point_id)): Path<(Uuid, Uuid, Uuid)>,
//     data: String,
// ) -> AppResp<()> {
//     match GLOBAL_MODBUS_MANAGER
//         .write_group_point_value(device_id, group_id, point_id, data)
//         .await
//     {
//         Ok(_) => AppResp::new(),
//         Err(e) => e.into(),
//     }
// }

async fn delete_group_variable(
    Path((device_id, group_id, variable_id)): Path<(Uuid, Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .delete_group_variable(device_id, group_id, variable_id)
        .await?;
    Ok(AppSuccess::empty())
}

async fn create_subscription(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSubscriptionReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .create_subscription(device_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_subscriptions(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchSubscriptionsResp>> {
    let data = GLOBAL_OPCUA_MANAGER
        .search_subscriptions(device_id, pagination)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_subscription(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSubscriptionReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .update_subscription(device_id, group_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_subscription(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .delete_subscription(device_id, group_id)
        .await?;
    Ok(AppSuccess::empty())
}

async fn create_event(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateEventReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .create_event(device_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_events(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchEventsResp>> {
    let data = GLOBAL_OPCUA_MANAGER
        .search_events(device_id, pagination)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_event(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateEventReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .update_event(device_id, group_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_event(
    Path((device_id, event_id)): Path<(Uuid, Uuid)>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .delete_event(device_id, event_id)
        .await?;
    Ok(AppSuccess::empty())
}

async fn create_sink(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .create_sink(device_id, None, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn search_sinks(
    Path(device_id): Path<Uuid>,
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchSinksResp>> {
    let data = GLOBAL_OPCUA_MANAGER
        .search_sinks(device_id, pagination)
        .await?;
    Ok(AppSuccess::data(data))
}

async fn update_sink(
    Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateSinkReq>,
) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER
        .update_sink(device_id, sink_id, req)
        .await?;
    Ok(AppSuccess::empty())
}

async fn delete_sink(Path((device_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResult<AppSuccess<()>> {
    GLOBAL_OPCUA_MANAGER.delete_sink(device_id, sink_id).await?;
    Ok(AppSuccess::empty())
}
