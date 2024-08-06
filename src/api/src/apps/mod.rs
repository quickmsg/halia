use apps::GLOBAL_APP_MANAGER;
use axum::{extract::Query, routing::get, Router};
use http_client::http_client_routes;
use mqtt_client::mqtt_client_routes;
use types::{
    apps::{SearchAppsResp, Summary},
    Pagination,
};

use crate::{AppResult, AppSuccess};

mod http_client;
mod mqtt_client;

pub fn routes() -> Router {
    Router::new()
        .route("/", get(search_apps))
        .route("/summary", get(get_apps_summary))
        .nest("/mqtt_client", mqtt_client_routes())
        .nest("/http_client", http_client_routes())
}

async fn search_apps(
    Query(pagination): Query<Pagination>,
) -> AppResult<AppSuccess<SearchAppsResp>> {
    let data = GLOBAL_APP_MANAGER.search(pagination).await?;
    Ok(AppSuccess::data(data))
}

async fn get_apps_summary() -> AppSuccess<Summary> {
    AppSuccess::data(GLOBAL_APP_MANAGER.get_summary().await)
}
