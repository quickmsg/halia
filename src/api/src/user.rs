use axum::{
    extract::State,
    routing::{post, put},
    Router,
};
// use tower::{Layer, Service};

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

// #[derive(Clone)]
// struct AuthLayer;

// impl<S> Layer<S> for AuthLayer {
//     type Service = MyMiddleware<S>;

//     fn layer(&self, inner: S) -> Self::Service {
//         MyMiddleware { inner }
//     }
// }

// #[derive(Clone)]
// struct MyMiddleware<S> {
//     inner: S,
// }

// impl<S> Service<Request> for MyMiddleware<S>
// where
//     S: Service<Request, Response = Response> + Send + 'static,
//     S::Future: Send + 'static,
// {
//     type Response = S::Response;
//     type Error = S::Error;
//     // `BoxFuture` is a type alias for `Pin<Box<dyn Future + Send + 'a>>`
//     type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

//     fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
//         self.inner.poll_ready(cx)
//     }

//     fn call(&mut self, request: Request) -> Self::Future {
//         let future = self.inner.call(request);
//         Box::pin(async move {
//             let response: Response = future.await?;
//             Ok(response)
//         })
//     }
// }
