use anyhow::{bail, Result};
use sqlx::prelude::FromRow;
use types::{
    schema::{CreateUpdateSchemaReq, ProtocolType, QueryParams, SchemaType},
    Pagination,
};

use super::POOL;

#[derive(FromRow)]
pub struct Schema {
    pub id: String,
    pub name: String,
    pub typ: i32,
    pub protocol_type: i32,
    // desc为关键字
    pub des: Option<Vec<u8>>,
    pub conf: Vec<u8>,
    pub ts: i64,
}

pub async fn init_table() -> Result<()> {
    sqlx::query(
        r#"  
CREATE TABLE IF NOT EXISTS schemas (
    id CHAR(32) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    des BLOB,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
    )
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn insert_name_exists(name: &String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name = ?")
        .bind(name)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count > 0)
}

pub async fn update_name_exists(id: &String, name: &String) -> Result<bool> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices WHERE name = ? AND id != ?")
        .bind(name)
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count > 0)
}

pub async fn insert(id: &String, req: CreateUpdateSchemaReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.ext)?;
    let ts = common::timestamp_millis();
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    sqlx::query(
        "INSERT INTO devices (id, status, err, typ, name, des, conf, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(id)
    .bind(desc)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;

    Ok(())
}

pub async fn read_one(id: &String) -> Result<Schema> {
    let schema = sqlx::query_as::<_, Schema>("SELECT * FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(schema)
}

pub async fn read_conf(id: &String) -> Result<Vec<u8>> {
    let conf: Vec<u8> = sqlx::query_scalar("SELECT conf FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(conf)
}

pub async fn search(
    pagination: Pagination,
    query_params: QueryParams,
) -> Result<(usize, Vec<Schema>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, schemas) = match (
        query_params.name,
        query_params.typ,
        query_params.protocol_type,
    ) {
        (None, None, None) => {
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM schemas")
                .fetch_one(POOL.get().unwrap())
                .await?;

            let schemas = sqlx::query_as::<_, Schema>(
                "SELECT * FROM schemas ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, schemas)
        }
        (None, None, Some(protocol_type)) => {
            let protocol_type = protocol_type_to_int(&protocol_type);
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM schemas WHERE protocol_type = ?")
                    .bind(protocol_type)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let schemas = sqlx::query_as::<_, Schema>(
                "SELECT * FROM schemas WHERE protocol_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(protocol_type)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, schemas)
        }
        (None, Some(typ), None) => {
            let typ = type_to_int(&typ);
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM schemas WHERE typ = ?")
                .bind(typ)
                .fetch_one(POOL.get().unwrap())
                .await?;

            let schemas = sqlx::query_as::<_, Schema>(
                "SELECT * FROM schemas WHERE typ = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(typ)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, schemas)
        }
        (None, Some(typ), Some(protocol_type)) => {
            let typ = type_to_int(&typ);
            let protocol_type = protocol_type_to_int(&protocol_type);
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM schemas WHERE typ = ? AND protocol_type = ?",
            )
            .bind(typ)
            .bind(protocol_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let schemas = sqlx::query_as::<_, Schema>(
                "SELECT * FROM schemas WHERE typ = ? AND protocol_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(typ)
            .bind(protocol_type)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, schemas)
        }
        (Some(name), None, None) => {
            let name = format!("%{}%", name);
            let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM schemas WHERE name LIKE ?")
                .bind(&name)
                .fetch_one(POOL.get().unwrap())
                .await?;

            let schemas = sqlx::query_as::<_, Schema>(
                "SELECT * FROM schemas WHERE name LIKE ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, schemas)
        }
        (Some(name), None, Some(protocol_type)) => {
            let name = format!("%{}%", name);
            let protocol_type = protocol_type_to_int(&protocol_type);
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM schemas WHERE name LIKE ? AND protocol_type = ?",
            )
            .bind(&name)
            .bind(protocol_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let schemas = sqlx::query_as::<_, Schema>(
                "SELECT * FROM schemas WHERE name LIKE ? AND protocol_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(protocol_type)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, schemas)
        }
        (Some(name), Some(typ), None) => {
            let name = format!("%{}%", name);
            let typ = type_to_int(&typ);
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM schemas WHERE name LIKE ? AND typ = ?")
                    .bind(&name)
                    .bind(typ)
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let schemas = sqlx::query_as::<_, Schema>(
                "SELECT * FROM schemas WHERE name LIKE ? AND typ = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, schemas)
        }
        (Some(name), Some(typ), Some(protocol_type)) => {
            let name = format!("%{}%", name);
            let typ = type_to_int(&typ);
            let protocol_type = protocol_type_to_int(&protocol_type);
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM schemas WHERE name LIKE ? AND typ = ? AND protocol_type = ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(protocol_type)
            .fetch_one(POOL.get().unwrap())
            .await?;

            let schemas = sqlx::query_as::<_, Schema>(
                "SELECT * FROM schemas WHERE name LIKE ? AND typ = ? AND protocol_type = ? ORDER BY ts DESC LIMIT ? OFFSET ?",
            )
            .bind(&name)
            .bind(typ)
            .bind(protocol_type)
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count as usize, schemas)
        }
    };

    Ok((count as usize, schemas))
}

pub async fn read_name(id: &String) -> Result<String> {
    let name: String = sqlx::query_scalar("SELECT name FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(name)
}

pub async fn read_type(id: &String) -> Result<i32> {
    let typ: i32 = sqlx::query_scalar("SELECT typ FROM devices WHERE id = ?")
        .bind(id)
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(typ)
}

pub async fn count_all() -> Result<usize> {
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM devices")
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count as usize)
}

// TODO 更新运行中的源和动作
pub async fn update(id: &String, req: CreateUpdateSchemaReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.ext)?;
    let desc = req.base.desc.map(|desc| desc.into_bytes());
    sqlx::query("UPDATE devices SET name = ?, des = ?, conf = ? WHERE id = ?")
        .bind(req.base.name)
        .bind(desc)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete(id: &String) -> Result<()> {
    sqlx::query("DELETE FROM schemas WHERE id = ?")
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

fn type_to_int(typ: &SchemaType) -> i32 {
    match typ {
        SchemaType::Code => 1,
        SchemaType::Decode => 2,
    }
}

fn int_to_type(typ: i32) -> Result<SchemaType> {
    match typ {
        1 => Ok(SchemaType::Code),
        2 => Ok(SchemaType::Decode),
        _ => bail!("未知类型: {}", typ),
    }
}

fn protocol_type_to_int(protocol_type: &ProtocolType) -> i32 {
    match protocol_type {
        ProtocolType::Avro => 1,
        ProtocolType::Protobuf => 2,
        ProtocolType::Csv => 3,
    }
}

fn int_to_protocol_type(protocol_type: i32) -> Result<ProtocolType> {
    match protocol_type {
        1 => Ok(ProtocolType::Avro),
        2 => Ok(ProtocolType::Protobuf),
        3 => Ok(ProtocolType::Csv),
        _ => bail!("未知协议类型: {}", protocol_type),
    }
}
