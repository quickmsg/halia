use axum::Json;
use managers::sink::SINK_MANAGER;
use types::rule::CreateSink;

pub(crate) async fn create_sink(Json(create_sink): Json<CreateSink>) -> (StatusCode, String) {
    match SINK_MANAGER.lock().unwrap().register(create_sink) {
        Ok(_) => return (StatusCode::CREATED, String::from("OK")),
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                String::from(format!("because: {}", e)),
            )
        }
    };
}
