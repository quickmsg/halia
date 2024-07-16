use apps::GLOBAL_APP_MANAGER;
use axum::{
    extract::Query,
    routing::{delete, get, post, put},
    Router,
};
use types::apps::SearchAppsResp;

use crate::{AppResp, Pagination};

mod mqtt_client;

pub fn routes() -> Router {
    Router::new()
        .route("/", get(search_apps))
        .nest("/mqtt_client", mqtt_client_routes())
}

async fn search_apps(pagination: Query<Pagination>) -> AppResp<SearchAppsResp> {
    match GLOBAL_APP_MANAGER.search(pagination.p, pagination.s).await {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}

fn mqtt_client_routes() -> Router {
    Router::new()
        .route("/", post(mqtt_client::create))
        .route("/:app_id", put(mqtt_client::update))
        .route("/:app_id", delete(mqtt_client::delete))
        .nest(
            "/:app_id",
            Router::new()
                .nest(
                    "/source",
                    Router::new()
                        .route("/", post(mqtt_client::create_source))
                        // .route("/", get(mqtt_client::search_sources))
                        .route("/:source_id", put(mqtt_client::update_source))
                        .route("/:source_id", delete(mqtt_client::delete_source)),
                )
                .nest(
                    "/sink",
                    Router::new()
                        .route("/", post(mqtt_client::create_sink))
                        // .route("/", get(mqtt_client::search_sinks))
                        .route("/:sink_id", put(mqtt_client::update_sink))
                        .route("/:sink_id", delete(mqtt_client::delete_sink)),
                ),
        )
}
