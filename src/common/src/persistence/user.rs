use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};

#[derive(FromRow)]
pub struct User {
    pub username: String,
    pub password: String,
}

pub async fn create_user(pool: &AnyPool, username: String, password: String) -> Result<()> {
    sqlx::query("INSERT INTO users (username, password) VALUES (?1, ?2)")
        .bind(username)
        .bind(password)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn read_user(pool: &AnyPool) -> Result<Option<User>> {
    let mut users = sqlx::query_as::<_, User>("SELECT username, password FROM users")
        .fetch_all(pool)
        .await?;

    if users.len() == 0 {
        Ok(None)
    } else {
        Ok(users.pop())
    }
}

pub async fn check_admin_exists(pool: &AnyPool) -> Result<bool> {
    let users =
        sqlx::query_as::<_, User>("SELECT username, password FROM users WHERE username = 'admin'")
            .fetch_all(pool)
            .await?;

    if users.len() == 0 {
        Ok(false)
    } else {
        Ok(true)
    }
}

// todo
pub async fn update_user_password(
    pool: &AnyPool,
    username: String,
    password: String,
    new_password: String,
) -> Result<()> {
    sqlx::query("UPDATE users SET password = ?1 WHERE username = ?2 AND passwowrd = ?3")
        .bind(new_password)
        .bind(username)
        .bind(password)
        .execute(pool)
        .await?;

    Ok(())
}
