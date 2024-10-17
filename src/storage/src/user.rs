use anyhow::Result;
use sqlx::prelude::FromRow;

use super::POOL;

pub static TABLE_NAME: &str = "users";

#[derive(FromRow)]
pub struct User {
    pub username: String,
    pub password: String,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS users (
    username VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL
);
"#,
    )
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn create_user(username: String, password: &String) -> Result<()> {
    sqlx::query("INSERT INTO users (username, password) VALUES (?, ?)")
        .bind(username)
        .bind(password)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn read_user() -> Result<Option<User>> {
    let mut users = sqlx::query_as::<_, User>("SELECT * FROM users")
        .fetch_all(POOL.get().unwrap())
        .await?;

    if users.len() == 0 {
        Ok(None)
    } else {
        Ok(users.pop())
    }
}

pub async fn check_admin_exists() -> Result<bool> {
    let users =
        sqlx::query_as::<_, User>("SELECT username, password FROM users WHERE username = 'admin'")
            .fetch_all(POOL.get().unwrap())
            .await?;

    if users.len() == 0 {
        Ok(false)
    } else {
        Ok(true)
    }
}

// todo
pub async fn update_user_password(
    username: String,
    password: String,
    new_password: String,
) -> Result<()> {
    sqlx::query("UPDATE users SET password = ? WHERE username = ? AND passwowrd = ?")
        .bind(new_password)
        .bind(username)
        .bind(password)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}
