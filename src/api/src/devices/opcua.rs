use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use devices::opcua::manager::GLOBAL_OPCUA_MANAGER;
use types::devices::opcua::{
    CreateUpdateGroupReq, CreateUpdateGroupVariableReq, CreateUpdateOpcuaReq,
    SearchGroupVariablesResp, SearchGroupsResp,
};
use uuid::Uuid;

use crate::{AppResp, Pagination};

pub(crate) fn opcua_routes() -> Router {
    Router::new()
        .route("/", post(create))
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
            "/:device_id/sink",
            Router::new(), // .route("/", post(create_sink))
                           // .route("/", get(search_sinks))
                           // .route("/:sink_id", put(update_sink))
                           // .route("/:sink_id", routing::delete(delete_sink)),
        )
}

async fn create(Json(req): Json<CreateUpdateOpcuaReq>) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER.create(None, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn update(Path(device_id): Path<Uuid>, Json(req): Json<CreateUpdateOpcuaReq>) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER.update(device_id, req).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn start(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER.start(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn stop(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER.stop(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete(Path(device_id): Path<Uuid>) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER.delete(device_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn create_group(
    Path(device_id): Path<Uuid>,
    Json(req): Json<CreateUpdateGroupReq>,
) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER
        .create_group(device_id, None, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn search_groups(
    Path(device_id): Path<Uuid>,
    pagination: Query<Pagination>,
) -> AppResp<SearchGroupsResp> {
    match GLOBAL_OPCUA_MANAGER
        .search_groups(device_id, pagination.p, pagination.s)
        .await
    {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

async fn update_group(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupReq>,
) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER
        .update_group(device_id, group_id, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn delete_group(Path((device_id, group_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER.delete_group(device_id, group_id).await {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn create_group_variable(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupVariableReq>,
) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER
        .create_group_variable(device_id, group_id, None, req)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

async fn search_group_variables(
    Path((device_id, group_id)): Path<(Uuid, Uuid)>,
    pagination: Query<Pagination>,
) -> AppResp<SearchGroupVariablesResp> {
    match GLOBAL_OPCUA_MANAGER
        .search_group_variables(device_id, group_id, pagination.p, pagination.s)
        .await
    {
        Ok(values) => AppResp::with_data(values),
        Err(e) => e.into(),
    }
}

async fn update_group_variable(
    Path((device_id, group_id, variable_id)): Path<(Uuid, Uuid, Uuid)>,
    Json(req): Json<CreateUpdateGroupVariableReq>,
) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER
        .update_group_variable(device_id, group_id, variable_id, req)
        .await
    {
        Ok(()) => AppResp::new(),
        Err(e) => e.into(),
    }
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
) -> AppResp<()> {
    match GLOBAL_OPCUA_MANAGER
        .delete_group_variable(device_id, group_id, variable_id)
        .await
    {
        Ok(_) => AppResp::new(),
        Err(e) => e.into(),
    }
}

// async fn create_sink(
//     Path(device_id): Path<Uuid>,
//     Json(req): Json<CreateUpdateSinkReq>,
// ) -> AppResp<()> {
//     match GLOBAL_MODBUS_MANAGER
//         .create_sink(device_id, None, req)
//         .await
//     {
//         Ok(_) => AppResp::new(),
//         Err(e) => e.into(),
//     }
// }

// async fn search_sinks(
//     Path(device_id): Path<Uuid>,
//     pagination: Query<Pagination>,
// ) -> AppResp<SearchSinksResp> {
//     match GLOBAL_MODBUS_MANAGER
//         .search_sinks(device_id, pagination.p, pagination.s)
//         .await
//     {
//         Ok(data) => AppResp::with_data(data),
//         Err(e) => e.into(),
//     }
// }

// async fn update_sink(
//     Path((device_id, sink_id)): Path<(Uuid, Uuid)>,
//     Json(req): Json<CreateUpdateSinkReq>,
// ) -> AppResp<()> {
//     match GLOBAL_MODBUS_MANAGER
//         .update_sink(device_id, sink_id, req)
//         .await
//     {
//         Ok(_) => AppResp::new(),
//         Err(e) => e.into(),
//     }
// }

// async fn delete_sink(Path((device_id, sink_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
//     match GLOBAL_MODBUS_MANAGER.delete_sink(device_id, sink_id).await {
//         Ok(_) => AppResp::new(),
//         Err(e) => e.into(),
//     }
// }
