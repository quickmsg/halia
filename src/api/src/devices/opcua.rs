use axum::{
    extract::{Path, Query},
    routing::{self, get, post, put},
    Json, Router,
};
use devices::opcua::manager::GLOBAL_OPCUA_MANAGER;
use types::devices::opcua::CreateUpdateOpcuaReq;
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
            "/:device_id",
            Router::new()
                .nest(
                    "/variable",
                    Router::new(), // .route("/", post(create_variable))
                                   // .route("/", get(search_variables))
                                   // .route("/:variable_id", put(update_variable))
                                   // .route("/:variable_id/value", put(write_group_point_value))
                                   // .route("/:variable_id", routing::delete(delete_variable)),
                )
                .nest(
                    "/sink",
                    Router::new(), // .route("/", post(create_sink))
                                   // .route("/", get(search_sinks))
                                   // .route("/:sink_id", put(update_sink))
                                   // .route("/:sink_id", routing::delete(delete_sink)),
                ),
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

// async fn create_variable(
//     Path(device_id): Path<Uuid>,
//     Json(req): Json<CreateUpdateVariableReq>,
// ) -> AppResp<()> {
//     match GLOBAL_OPCUA_MANAGER
//         .create_variable(device_id, None, req)
//         .await
//     {
//         Ok(_) => AppResp::new(),
//         Err(e) => e.into(),
//     }
// }

// async fn search_variables(
//     Path(device_id): Path<Uuid>,
//     pagination: Query<Pagination>,
// ) -> AppResp<SearchVariablesResp> {
//     match GLOBAL_OPCUA_MANAGER
//         .search_variables(device_id, pagination.p, pagination.s)
//         .await
//     {
//         Ok(values) => AppResp::with_data(values),
//         Err(e) => e.into(),
//     }
// }

// async fn update_variable(
//     Path((device_id, variable_id)): Path<(Uuid, Uuid)>,
//     Json(req): Json<CreateUpdateVariableReq>,
// ) -> AppResp<()> {
//     match GLOBAL_OPCUA_MANAGER
//         .update_variable(device_id, variable_id, req)
//         .await
//     {
//         Ok(()) => AppResp::new(),
//         Err(e) => e.into(),
//     }
// }

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

// async fn delete_variable(Path((device_id, variable_id)): Path<(Uuid, Uuid)>) -> AppResp<()> {
//     match GLOBAL_OPCUA_MANAGER
//         .delete_variable(device_id, variable_id)
//         .await
//     {
//         Ok(()) => AppResp::new(),
//         Err(e) => e.into(),
//     }
// }

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
