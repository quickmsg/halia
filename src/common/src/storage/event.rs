use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use uuid::Uuid;

#[derive(FromRow)]
pub struct Event {
    pub id: String,
    pub source_type: i32,
    pub event_type: i32,
    pub info: Option<String>,
}

pub async fn create_event(
    pool: &AnyPool,
    id: &Uuid,
    source_type: i32,
    event_type: i32,
    info: Option<String>,
) -> Result<()> {
    match info {
        Some(info) => {
            sqlx::query(
                "INSERT INTO events (id, source_type, event_type, info) VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(id.to_string())
            .bind(source_type)
            .bind(event_type)
            .bind(info)
            .execute(pool)
            .await?;
        }
        None => {
            sqlx::query("INSERT INTO events (id, source_type, event_type) VALUES (?1, ?2, ?3)")
                .bind(id.to_string())
                .bind(source_type)
                .bind(event_type)
                .execute(pool)
                .await?;
        }
    }

    Ok(())
}

pub async fn read_events(
    pool: &AnyPool,
    name: Option<String>,
    source_type: Option<i32>,
    event_type: Option<i32>,
    page: usize,
    size: usize,
) -> Result<Vec<Event>> {
    let events = sqlx::query_as::<_, Event>("SELECT id, status, conf FROM events")
        .fetch_all(pool)
        .await?;

    Ok(events)
}

// pub async fn delete_device(pool: &AnyPool, id: &Uuid) -> Result<()> {
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

//     Ok(())
// }
