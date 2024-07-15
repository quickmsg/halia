use axum::{
    routing::{delete, get, post, put},
    Router,
};

pub fn routes() -> Router {
    Router::new()
        .route("/", post(apps::create_app))
        .route("/search", get(apps::search_apps))
        .route("/:app_id", put(apps::update_app))
        .route("/:app_id", delete(apps::delete_app))
        .nest(
            "/:app_id/source",
            Router::new()
                .route("/", post(apps::create_source))
                .route("/search", get(apps::search_sources))
                .route("/:source_id", put(apps::update_source))
                .route("/:source_id", delete(apps::delete_source)),
        )
        .nest(
            "/:app_id/sink",
            Router::new()
                .route("/", post(apps::create_sink))
                .route("/search", get(apps::search_sinks))
                .route("/:sink_id", put(apps::update_sink))
                .route("/:sink_id", delete(apps::delete_sink)),
        )
}

fn mqtt_client_routes() -> Router {
}
