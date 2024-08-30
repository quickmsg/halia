use axum::{
    extract::State,
    routing::{post, put},
    Router,
};

use crate::{AppResult, AppState, AppSuccess};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/registration", post(registration))
        .route("/login", post(login))
        .route("/password", put(password))
}

async fn registration(State(state): State<AppState>, body: String) -> AppResult<AppSuccess<()>> {
    Ok(AppSuccess::empty())
}

async fn login(State(state): State<AppState>) -> AppSuccess<()> {
    // AppSuccess::data(rules)
    todo!()
}

async fn password(State(state): State<AppState>) -> AppSuccess<()> {
    todo!()
}
