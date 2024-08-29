use anyhow::Result;
use sqlx::{AnyPool, Row};
use uuid::Uuid;

pub struct Sink {
    pub id: String,
    pub conf: String,
}

pub async fn create_sink(pool: &AnyPool, parent_id: &Uuid, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("INSERT INTO sinks (id, parent_id, conf) VALUES (?1, ?2, ?3)")
        .bind(id.to_string())
        .bind(parent_id.to_string())
        .bind(conf)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn read_sinks(pool: &AnyPool, parent_id: &Uuid) -> Result<Vec<Sink>> {
    let rows = sqlx::query("SELECT id, conf FROM sinks WHERE parent_id = ?1")
        .bind(parent_id.to_string())
        .fetch_all(pool)
        .await?;

    let mut sinks = vec![];
    for row in rows {
        let id: String = row.get(0);
        let conf: String = row.get(1);
        sinks.push(Sink { id, conf })
    }

    Ok(sinks)
}

pub async fn update_sink(pool: &AnyPool, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("UPDATE sinks SET conf = ?1 WHERE id = ?2")
        .bind(conf)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_sink(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query("DELETE sinks WHERE id = ?1")
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}
