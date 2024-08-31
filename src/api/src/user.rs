use std::task::{Context, Poll};

use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
    routing::{post, put},
    Router,
};
use futures_util::future::BoxFuture;
use tower::{Layer, Service};
use tracing::debug;

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

pub async fn auth(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let b = headers.get("Authorization");
    if b.is_none() {
        Err(StatusCode::UNAUTHORIZED)
    } else {
        let response = next.run(request).await;
        Ok(response)
    }
}

#[derive(Clone)]
pub struct AuthLayer {
    pub state: AppState,
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthMiddleware {
            inner,
            state: self.state.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    state: AppState,
}

impl<S> Service<Request> for AuthMiddleware<S>
where
    S: Service<Request, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    // `BoxFuture` is a type alias for `Pin<Box<dyn Future + Send + 'a>>`
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        debug!("{:?}", request);
        let headers = request.headers();
        let b = headers.get("Authorization");
        if b.is_none() {
            // let resp = Response::builder().status(StatusCode::OK).body() ;
        }
        debug!("{:?}", b);
        let future = self.inner.call(request);
        Box::pin(async move {
            let response: Response = future.await?;
            Ok(response)
        })
    }
}
