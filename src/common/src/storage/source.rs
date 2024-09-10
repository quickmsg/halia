use anyhow::Result;
use sqlx::{AnyPool, Row};
use types::CreateUpdateSourceOrSinkReq;
use uuid::Uuid;

pub struct Source {
    pub id: String,
    pub conf: String,
}

pub async fn create_source(
    pool: &AnyPool,
    parent_id: &Uuid,
    id: &Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> Result<()> {
    sqlx::query("INSERT INTO sources (id, parent_id, conf) VALUES (?1, ?2, ?3)")
        .bind(id.to_string())
        .bind(parent_id.to_string())
        // todo
        // .bind(conf)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn read_sources(pool: &AnyPool, parent_id: &Uuid) -> Result<Vec<Source>> {
    let rows = sqlx::query("SELECT id, conf FROM sources WHERE parent_id = ?1")
        .bind(parent_id.to_string())
        .fetch_all(pool)
        .await?;
    let mut sources = vec![];
    for row in rows {
        let id: String = row.get(0);
        let conf: String = row.get(1);
        sources.push(Source { id, conf });
    }

    Ok(sources)
}

pub async fn update_source(pool: &AnyPool, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("UPDATE sources SET conf = ?1 WHERE id = ?2")
        .bind(conf)
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_source(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM sources WHERE id = ?1")
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}
