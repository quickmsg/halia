use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct User {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct Password {
    pub password: String,
}

#[derive(Serialize)]
pub struct AuthInfo {
    pub token: String,
}

#[derive(Serialize)]
pub struct AdminExists {
    pub exists: bool,
}

#[derive(Deserialize)]
pub struct UpdatePassword {
    pub password: String,
    pub new_password: String,
}