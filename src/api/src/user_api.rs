use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::Response,
    routing::{get, post, put},
    Json, Router,
};
use common::{error::HaliaError, persistence};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use tracing::warn;
use types::user::{AdminExists, AuthInfo, Password, UpdatePassword, User};

use crate::{AppError, AppResult, AppState, AppSuccess, EMPTY_USER_CODE, WRONG_PASSWORD_CODE};

const SECRET: &str = "must be random,todo";

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub username: String,
    #[serde(with = "jwt_numeric_date")]
    iat: OffsetDateTime,
    #[serde(with = "jwt_numeric_date")]
    exp: OffsetDateTime,
}

impl Claims {
    pub fn new(username: String, iat: OffsetDateTime, exp: OffsetDateTime) -> Self {
        // normalize the timestamps by stripping of microseconds
        let iat = iat
            .date()
            .with_hms_milli(iat.hour(), iat.minute(), iat.second(), 0)
            .unwrap()
            .assume_utc();
        let exp = exp
            .date()
            .with_hms_milli(exp.hour(), exp.minute(), exp.second(), 0)
            .unwrap()
            .assume_utc();

        Self { username, iat, exp }
    }
}

mod jwt_numeric_date {
    //! Custom serialization of OffsetDateTime to conform with the JWT spec (RFC 7519 section 2, "Numeric Date")
    use serde::{self, Deserialize, Deserializer, Serializer};
    use time::OffsetDateTime;

    /// Serializes an OffsetDateTime to a Unix timestamp (milliseconds since 1970/1/1T00:00:00T)
    pub fn serialize<S>(date: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let timestamp = date.unix_timestamp();
        serializer.serialize_i64(timestamp)
    }

    /// Attempts to deserialize an i64 and use as a Unix timestamp
    pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        OffsetDateTime::from_unix_timestamp(i64::deserialize(deserializer)?)
            .map_err(|_| serde::de::Error::custom("invalid Unix timestamp value"))
    }
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/emptyuser", get(check_empty_user))
        .route("/registration", post(registration))
        .route("/login", post(login))
        .route("/password", put(password))
}

async fn check_empty_user(State(state): State<AppState>) -> AppResult<AppSuccess<AdminExists>> {
    let exists = persistence::user::check_admin_exists(&state.pool)
        .await
        .map_err(|e| HaliaError::Common(e.to_string()))?;

    Ok(AppSuccess::data(AdminExists { exists }))
}

async fn registration(
    State(state): State<AppState>,
    Json(password): Json<Password>,
) -> AppResult<AppSuccess<()>> {
    let exists = persistence::user::check_admin_exists(&state.pool)
        .await
        .map_err(|e| HaliaError::Common(e.to_string()))?;

    if exists {
        return Err(AppError::new(1, "管理员账户已存在！".to_string()));
    }

    persistence::user::create_user(&state.pool, "admin".to_string(), password.password)
        .await
        .map_err(|e| HaliaError::Common(e.to_string()))?;

    Ok(AppSuccess::empty())
}

async fn login(
    State(state): State<AppState>,
    Json(user): Json<User>,
) -> AppResult<AppSuccess<AuthInfo>> {
    let db_user = match persistence::user::read_user(&state.pool).await {
        Ok(user) => match user {
            Some(user) => user,
            None => {
                return Err(AppError::new(
                    EMPTY_USER_CODE,
                    "数据库无账户，请注册！".to_string(),
                ))
            }
        },
        Err(e) => return Err(AppError::new(1, e.to_string())),
    };

    if db_user.username != user.username || db_user.password != user.password {
        return Err(AppError::new(
            WRONG_PASSWORD_CODE,
            "账户或密码错误！".to_string(),
        ));
    }

    let iat = OffsetDateTime::now_utc();
    let exp = iat + Duration::hours(2);
    let claims = Claims::new(user.username, iat, exp);

    let header = Header {
        kid: Some("signing_key".to_owned()),
        alg: Algorithm::HS512,
        ..Default::default()
    };
    let token = match encode(&header, &claims, &EncodingKey::from_secret(SECRET.as_ref())) {
        Ok(t) => t,
        Err(_) => panic!(), // in practice you would return the error
    };

    Ok(AppSuccess::data(AuthInfo { token }))
}

async fn password(
    State(state): State<AppState>,
    Json(update_password): Json<UpdatePassword>,
) -> AppResult<AppSuccess<()>> {
    // 从token中读取
    let username = "xxs".to_owned();
    match persistence::user::update_user_password(
        &state.pool,
        username,
        update_password.password,
        update_password.new_password,
    )
    .await
    {
        Ok(_) => Ok(AppSuccess::empty()),
        Err(e) => {
            warn!("{}", e);
            return Err(AppError::new(1, "密码错误".to_owned()));
        }
    }
}

pub async fn auth(
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let token = match headers.get("Authorization") {
        Some(t) => t.to_str().or_else(|_| Err(StatusCode::UNAUTHORIZED))?,
        None => return Err(StatusCode::UNAUTHORIZED),
    };
    let token_data = match decode::<Claims>(
        token,
        &DecodingKey::from_secret(SECRET.as_ref()),
        &Validation::new(Algorithm::HS512),
    ) {
        Ok(c) => c,
        Err(e) => {
            warn!("{:?}", e);
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    // debug!("{:?}", token_data);

    let response = next.run(request).await;
    Ok(response)
}
