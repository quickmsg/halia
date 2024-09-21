use anyhow::Result;
use sqlx::prelude::FromRow;

use super::POOL;

#[derive(FromRow)]
pub struct User {
    pub username: String,
    pub password: String,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS users (
    username VARCHAR(255) NOT NULL,  -- 使用 VARCHAR(255) 代替 TEXT
    password VARCHAR(255) NOT NULL   -- 使用 VARCHAR(255) 代替 TEXT
);
"#,
    )
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn create_user(username: String, password: String) -> Result<()> {
    sqlx::query("INSERT INTO users (username, password) VALUES (?1, ?2)")
        .bind(username)
        .bind(password)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn read_user() -> Result<Option<User>> {
    let mut users = sqlx::query_as::<_, User>("SELECT username, password FROM users")
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
    sqlx::query("UPDATE users SET password = ?1 WHERE username = ?2 AND passwowrd = ?3")
        .bind(new_password)
        .bind(username)
        .bind(password)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}
