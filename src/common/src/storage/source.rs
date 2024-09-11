use anyhow::Result;
use sqlx::{any::AnyArguments, query::Query, Any, AnyPool, FromRow};
use types::{CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams};
use uuid::Uuid;

#[derive(FromRow)]
pub struct Source {
    pub id: String,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
    pub ts: i64,
    pub rule_ref_cnt: i32,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    parent_id TEXT NOT NULL,
    name TEXT NOT NULL,
    desc TEXT,
    conf TEXT NOT NULL,
    ts INT NOT NULL,
    rule_ref_cnt INT NOT NULL
);
"#,
    )
    .execute(storage)
    .await?;

    Ok(())
}

pub async fn create_source(
    pool: &AnyPool,
    parent_id: &Uuid,
    id: &Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> Result<()> {
    let conf = serde_json::to_string(&req.ext)?;
    let ts = chrono::Utc::now().timestamp();
    let query: Query<'_, Any, AnyArguments> = match req.base.desc {
        Some(desc) => {
            sqlx::query(
                r#"INSERT INTO sources (desc, id, parent_id, name, conf, ts, rule_ref) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
            )
            .bind(desc.to_string())
        }
        None => {
            sqlx::query(
                r#"INSERT INTO sources (id, parent_id, name, conf, ts, rule_ref) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"#,
            )
        }
    };
    query
        .bind(id.to_string())
        .bind(parent_id.to_string())
        .bind(req.base.name)
        .bind(conf)
        .bind(ts)
        .bind(0)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn read_all_sources(pool: &AnyPool, parent_id: &Uuid) -> Result<Vec<Source>> {
    Ok(
        sqlx::query_as::<_, Source>("SELECT * FROM sources WHERE parent_id = ?1")
            .bind(parent_id.to_string())
            .fetch_all(pool)
            .await?,
    )
}

pub async fn read_source(storage: &AnyPool, id: &Uuid) -> Result<Source> {
    let source = sqlx::query_as::<_, Source>("SELECT * FROM sources WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(storage)
        .await?;
    Ok(source)
}

pub async fn search_sources(
    storage: &AnyPool,
    parent_id: &Uuid,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> Result<(usize, Vec<Source>)> {
    let (count, sources) = match query.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sources WHERE parent_id = ?1 AND name LIKE ?2",
            )
            .bind(parent_id.to_string())
            .bind(format!("%{}%", name))
            .fetch_one(storage)
            .await?;

            let sources = sqlx::query_as::<_, Source>(
                "SELECT * FROM sources WHERE parent_id = ?1 AND name LIKE ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            ).bind(parent_id.to_string())
            .bind(format!("%{}%", name))
            .fetch_all(storage).await?;

            (count, sources)
        }
        None => {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM sources WHERE parent_id = ?1")
                    .bind(parent_id.to_string())
                    .fetch_one(storage)
                    .await?;
            let sources = sqlx::query_as::<_, Source>(
                "SELECT * FROM sources WHERE parent_id = ?1 ORDER BY ts DESC LIMIT ?2 OFFSET ?3",
            )
            .bind(parent_id.to_string())
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count, sources)
        }
    };

    Ok((count as usize, sources))
}

pub async fn count_by_parent_id(storage: &AnyPool, parent_id: &Uuid) -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sources WHERE parent_id = ?1")
        .bind(parent_id.to_string())
        .fetch_one(storage)
        .await?;
    Ok(count as usize)
}

// TODO
pub async fn add_rule_ref(storage: &AnyPool, source_id: &Uuid, rule_id: &Uuid) -> Result<usize> {
    // let rule_ref: i32 = sqlx::query_scalar("SELECT rule_ref FROM sources WHERE id = ?1")
    //     .bind(id.to_string())
    //     .fetch_one(storage)
    //     .await?;
    // Ok(rule_ref as usize)
    todo!()
}

// TODO
pub async fn del_rule_ref(storage: &AnyPool, source_id: &Uuid, rule_id: &Uuid) -> Result<usize> {
    // let rule_ref: i32 = sqlx::query_scalar("SELECT rule_ref FROM sources WHERE id = ?1")
    //     .bind(id.to_string())
    //     .fetch_one(storage)
    //     .await?;
    // Ok(rule_ref as usize)
    todo!()
}

pub async fn read_rule_ref(storage: &AnyPool, id: &Uuid) -> Result<usize> {
    let rule_ref: i32 = sqlx::query_scalar("SELECT rule_ref FROM sources WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(storage)
        .await?;
    Ok(rule_ref as usize)
}

pub async fn read_conf(pool: &AnyPool, id: &Uuid) -> Result<String> {
    let conf: String = sqlx::query_scalar("SELECT conf FROM sources WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(pool)
        .await?;
    Ok(conf)
}

pub async fn update_source(
    pool: &AnyPool,
    id: &Uuid,
    req: CreateUpdateSourceOrSinkReq,
) -> Result<()> {
    let conf = serde_json::to_string(&req.ext)?;
    match req.base.desc {
        Some(desc) => {
            sqlx::query("UPDATE sources SET name = ?1, desc = ?2, conf = ?3 WHERE id = ?4")
                .bind(req.base.name)
                .bind(desc)
                .bind(conf)
                .bind(id.to_string())
                .execute(pool)
                .await?;
        }
        None => {
            sqlx::query("UPDATE sources SET name = ?1, conf = ?3 WHERE id = ?4")
                .bind(req.base.name)
                .bind(conf)
                .bind(id.to_string())
                .execute(pool)
                .await?;
        }
    }

    Ok(())
}

pub async fn delete_source(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM sources WHERE id = ?1")
        .bind(id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn check_delete_all(storage: &AnyPool, parent_id: &Uuid) -> Result<bool> {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sources WHERE parent_id = ?1 AND rule_ref = 0")
            .bind(parent_id.to_string())
            .fetch_one(storage)
            .await?;

    Ok(count == 0)
}
