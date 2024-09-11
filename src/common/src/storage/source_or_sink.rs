use anyhow::Result;
use sqlx::{any::AnyArguments, query::Query, Any, AnyPool, FromRow};
use types::{CreateUpdateSourceOrSinkReq, Pagination, QuerySourcesOrSinksParams};
use uuid::Uuid;

pub enum Type {
    Source,
    Sink,
}

impl From<i32> for Type {
    fn from(i: i32) -> Self {
        match i {
            1 => Type::Source,
            2 => Type::Sink,
            _ => panic!("invalid type"),
        }
    }
}

impl Into<i32> for Type {
    fn into(self) -> i32 {
        match self {
            Type::Source => 1,
            Type::Sink => 2,
        }
    }
}

#[derive(FromRow)]
pub struct SourceOrSink {
    pub id: String,
    pub typ: i64,
    pub name: String,
    pub desc: Option<String>,
    pub conf: String,
    pub ts: i64,
}

pub async fn init_table(storage: &AnyPool) -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS sources_or_sinks (
    id TEXT PRIMARY KEY,
    typ INT NOT NULL,
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

pub async fn create(
    pool: &AnyPool,
    parent_id: &Uuid,
    id: &Uuid,
    typ: Type,
    req: CreateUpdateSourceOrSinkReq,
) -> Result<()> {
    let typ: i32 = typ.into();
    let conf = serde_json::to_string(&req.ext)?;
    let ts = chrono::Utc::now().timestamp();
    let query: Query<'_, Any, AnyArguments> = match req.base.desc {
        Some(desc) => {
            sqlx::query(
                r#"INSERT INTO sources_or_sinks (desc, id, typ, parent_id, name, conf, ts, rule_ref) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"#,
            )
            .bind(desc.to_string())
        }
        None => {
            sqlx::query(
                r#"INSERT INTO sources (id, typ, parent_id, name, conf, ts, rule_ref) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"#,
            )
        }
    };
    query
        .bind(id.to_string())
        .bind(typ)
        .bind(parent_id.to_string())
        .bind(req.base.name)
        .bind(conf)
        .bind(ts)
        .bind(0)
        .execute(pool)
        .await?;

    Ok(())
}

pub async fn read_all_by_parent_id(
    storage: &AnyPool,
    parent_id: &Uuid,
    typ: Type,
) -> Result<Vec<SourceOrSink>> {
    let typ: i32 = typ.into();
    let sources_or_sinks = sqlx::query_as::<_, SourceOrSink>(
        "SELECT * FROM sources_or_sinks WHERE parent_id = ?1 AND typ = ?2",
    )
    .bind(parent_id.to_string())
    .bind(typ)
    .fetch_all(storage)
    .await?;
    Ok(sources_or_sinks)
}

pub async fn read(storage: &AnyPool, id: &Uuid) -> Result<SourceOrSink> {
    let source_or_sink =
        sqlx::query_as::<_, SourceOrSink>("SELECT * FROM sources_or_sinks WHERE id = ?1")
            .bind(id.to_string())
            .fetch_one(storage)
            .await?;
    Ok(source_or_sink)
}

pub async fn search(
    storage: &AnyPool,
    parent_id: &Uuid,
    typ: Type,
    pagination: Pagination,
    query: QuerySourcesOrSinksParams,
) -> Result<(usize, Vec<SourceOrSink>)> {
    let typ: i32 = typ.into();
    let (count, sources_or_sinks) = match query.name {
        Some(name) => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sources_or_sinks WHERE parent_id = ?1 AND typ = ?2 AND name LIKE ?3",
            )
            .bind(parent_id.to_string())
            .bind(typ)
            .bind(format!("%{}%", name))
            .fetch_one(storage)
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, SourceOrSink>(
                "SELECT * FROM sources_or_sinks WHERE parent_id = ?1 AND AND typ = ?2 AND name LIKE ?3 ORDER BY ts DESC LIMIT ?4 OFFSET ?5",
            ).bind(parent_id.to_string())
            .bind(typ)
            .bind(format!("%{}%", name))
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage).await?;

            (count, sources_or_sinks)
        }
        None => {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM sources_or_sinks WHERE parent_id = ?1 AND typ = ?2",
            )
            .bind(parent_id.to_string())
            .bind(typ)
            .fetch_one(storage)
            .await?;

            let sources_or_sinks = sqlx::query_as::<_, SourceOrSink>(
                "SELECT * FROM sources_or_sinks WHERE parent_id = ?1 AND typ = ?2 ORDER BY ts DESC LIMIT ?3 OFFSET ?4",
            )
            .bind(parent_id.to_string())
            .bind(typ)
            .bind(pagination.size as i64)
            .bind(((pagination.page - 1) * pagination.size) as i64)
            .fetch_all(storage)
            .await?;

            (count, sources_or_sinks)
        }
    };

    Ok((count as usize, sources_or_sinks))
}

pub async fn count_by_parent_id(storage: &AnyPool, parent_id: &Uuid, typ: Type) -> Result<usize> {
    let typ: i32 = typ.into();
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sources_or_sinks WHERE parent_id = ?1 AND typ = ?2",
    )
    .bind(parent_id.to_string())
    .bind(typ)
    .fetch_one(storage)
    .await?;
    Ok(count as usize)
}

pub async fn read_conf_by_id(storage: &AnyPool, id: &Uuid) -> Result<String> {
    let conf: String = sqlx::query_scalar("SELECT conf FROM sources_or_sinks WHERE id = ?1")
        .bind(id.to_string())
        .fetch_one(storage)
        .await?;
    Ok(conf)
}

pub async fn update(pool: &AnyPool, id: &Uuid, req: CreateUpdateSourceOrSinkReq) -> Result<()> {
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

pub async fn delete_by_id(storage: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM sources_or_sinks WHERE id = ?1")
        .bind(id.to_string())
        .execute(storage)
        .await?;
    Ok(())
}

pub async fn delete_by_parent_id(storage: &AnyPool, parent_id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM sources_or_sinks WHERE parent_id = ?1")
        .bind(parent_id.to_string())
        .execute(storage)
        .await?;
    Ok(())
}

// TODO
pub async fn check_delete_all(storage: &AnyPool, parent_id: &Uuid) -> Result<bool> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sources_or_sinks WHERE parent_id = ?1 AND rule_ref_cnt = 0",
    )
    .bind(parent_id.to_string())
    .fetch_one(storage)
    .await?;

    Ok(count == 0)
}

pub async fn check_exists(storage: &AnyPool, source_or_sink_id: &Uuid) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sources_or_sinks WHERE id = ?1")
        .bind(source_or_sink_id.to_string())
        .fetch_one(storage)
        .await?;

    Ok(count == 1)
}
