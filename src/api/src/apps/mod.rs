use apps::GLOBAL_APP_MANAGER;
use axum::{extract::Query, routing::get, Router};
use mqtt_client::mqtt_client_routes;
use types::apps::SearchAppsResp;

use crate::{AppResp, Pagination};

mod mqtt_client;

pub fn routes() -> Router {
    Router::new().nest(
        "/app",
        Router::new()
            .route("/", get(search_apps))
            .nest("/mqtt_client", mqtt_client_routes()),
    )
}

async fn search_apps(pagination: Query<Pagination>) -> AppResp<SearchAppsResp> {
    match GLOBAL_APP_MANAGER.search(pagination.p, pagination.s).await {
        Ok(data) => AppResp::with_data(data),
        Err(e) => e.into(),
    }
}
