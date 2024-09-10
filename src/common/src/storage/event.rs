use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use types::{events::QueryParams, Pagination};
use uuid::Uuid;

#[derive(FromRow)]
pub struct Event {
    pub id: String,
    pub source_type: i32,
    pub event_type: i32,
    pub info: Option<String>,
}

pub async fn create_talbe(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    source_type INTEGER NOT NULL,
    event_type INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    info TEXT,
);
"#,
    )
    .execute(storage)
    .await?;

    Ok(())
}

pub async fn create_event(
    pool: &AnyPool,
    id: &Uuid,
    source_type: i32,
    event_type: i32,
    info: Option<String>,
) -> Result<()> {
    let ts = chrono::Utc::now().timestamp();
    match info {
        Some(info) => {
            sqlx::query(
                "INSERT INTO events (id, source_type, event_type, ts, info) VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .bind(id.to_string())
            .bind(source_type)
            .bind(event_type)
            .bind(ts)
            .bind(info)
            .execute(pool)
            .await?;
        }
        None => {
            sqlx::query("INSERT INTO events (id, source_type, event_type) VALUES (?1, ?2, ?3, ?4)")
                .bind(id.to_string())
                .bind(source_type)
                .bind(event_type)
                .bind(ts)
                .execute(pool)
                .await?;
        }
    }

    Ok(())
}

pub async fn search_events(
    storage: &AnyPool,
    query_params: QueryParams,
    pagination: Pagination,
) -> Result<(Vec<Event>, i64)> {
    // match query_params.event_type {
    //     Some(_) => todo!(),
    //     None => todo!(),
    // }
    let offset = (pagination.page - 1) * pagination.size;
    let events =
        sqlx::query_as::<_, Event>("SELECT * FROM events ORDER BY ts DESC LIMIT ? OFFSET ?")
            .bind(pagination.size as i64)
            .bind(offset as i64)
            .fetch_all(storage)
            .await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(storage)
        .await?;

    Ok((events, count))
}

// todo
pub async fn delete_exipired_events(storage: &AnyPool) -> Result<()> {
    //     sqlx::query(
    //         r#"
    // DELETE FROM devices WHERE id = ?1;
    // DELETE FROM sources WHERE parent_id = ?1;
    // DELETE FROM sinks WHERE parent_id = ?1;
    //     "#,
    //     )
    //     .bind(id.to_string())
    //     .execute(pool)
    //     .await?;

    Ok(())
}
