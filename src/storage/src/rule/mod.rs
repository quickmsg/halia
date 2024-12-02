use anyhow::Result;
use common::error::HaliaResult;
use sqlx::{
    any::AnyArguments,
    prelude::FromRow,
    query::{QueryAs, QueryScalar},
    Any,
};
use types::{
    rules::{Conf, CreateUpdateRuleReq, QueryParams},
    Pagination, Status,
};

use super::POOL;

pub mod reference;

static TABLE_NAME: &str = "rules";

#[derive(FromRow)]
struct DbRule {
    pub id: String,
    pub name: String,
    pub conf: Vec<u8>,
    pub status: i32,
    pub ts: i64,
}

impl DbRule {
    pub fn transfer(self) -> Result<Rule> {
        Ok(Rule {
            id: self.id,
            name: self.name,
            conf: serde_json::from_slice(&self.conf)?,
            status: self.status.try_into()?,
            ts: self.ts,
        })
    }
}

pub struct Rule {
    pub id: String,
    pub name: String,
    pub conf: Conf,
    pub status: Status,
    pub ts: i64,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    id CHAR(32) PRIMARY KEY,
    status SMALLINT NOT NULL,
    name VARCHAR(255) NOT NULL UNIQUE,
    conf BLOB NOT NULL,
    ts BIGINT UNSIGNED NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(id: &String, req: CreateUpdateRuleReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.conf)?;
    let ts = common::timestamp_millis() as i64;
    sqlx::query(
        format!(
            "INSERT INTO {} (id, status, name, conf, ts) VALUES (?, ?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(id)
    .bind(Into::<i32>::into(Status::Stopped))
    .bind(req.name)
    .bind(conf)
    .bind(ts)
    .execute(POOL.get().unwrap())
    .await?;
    Ok(())
}

pub async fn read_one(id: &String) -> Result<Rule> {
    let db_rule =
        sqlx::query_as::<_, DbRule>(format!("SELECT * FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;
    db_rule.transfer()
}

pub async fn read_conf(id: &String) -> Result<serde_json::Value> {
    let conf: Vec<u8> =
        sqlx::query_scalar(format!("SELECT conf FROM {} WHERE id = ?", TABLE_NAME).as_str())
            .bind(id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    let conf = serde_json::from_slice(&conf)?;
    Ok(conf)
}

pub async fn read_all_on() -> Result<Vec<Rule>> {
    let db_rules = sqlx::query_as::<_, DbRule>(
        format!("SELECT * FROM {} WHERE status = ?", TABLE_NAME).as_str(),
    )
    .bind(Into::<i32>::into(Status::Running))
    .fetch_all(POOL.get().unwrap())
    .await?;

    let rules = db_rules
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<Rule>>>()?;

    Ok(rules)
}

pub async fn get_summary() -> Result<(usize, usize)> {
    let (total, running_cnt, _) = crate::get_summary(TABLE_NAME).await?;
    Ok((total as usize, running_cnt as usize))
}

pub async fn search(pagination: Pagination, query: QueryParams) -> Result<(usize, Vec<Rule>)> {
    let (limit, offset) = pagination.to_sql();
    let (count, db_rules) = match (
        &query.name,
        &query.status,
        &query.parent_id,
        &query.resource_id,
    ) {
        (None, None, None, None) => {
            let count: i64 =
                sqlx::query_scalar(format!("SELECT COUNT(*) FROM {}", TABLE_NAME).as_str())
                    .fetch_one(POOL.get().unwrap())
                    .await?;

            let db_rules = sqlx::query_as::<_, DbRule>(
                format!(
                    "SELECT * FROM {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(POOL.get().unwrap())
            .await?;

            (count, db_rules)
        }
        _ => {
            let mut where_clause = String::new();

            if query.name.is_some() {
                where_clause.push_str("WHERE name LIKE ?");
            }
            if query.status.is_some() {
                if where_clause.is_empty() {
                    where_clause.push_str("WHERE status = ?");
                } else {
                    where_clause.push_str(" AND status = ?");
                }
            }
            if query.parent_id.is_some() {
                if where_clause.is_empty() {
                    where_clause.push_str(
                        format!(
                            "WHERE id IN (SELECT id FROM {} WHERE parent_id = ?)",
                            reference::TABLE_NAME
                        )
                        .as_str(),
                    );
                } else {
                    where_clause.push_str(
                        format!(
                            " AND id IN (SELECT id FROM {} WHERE parent_id = ?)",
                            reference::TABLE_NAME
                        )
                        .as_str(),
                    );
                }
            }
            if query.resource_id.is_some() {
                match where_clause.is_empty() {
                    true => {
                        where_clause.push_str(
                            format!(
                                "WHERE id IN (SELECT id FROM {} WHERE resource_id = ?)",
                                reference::TABLE_NAME
                            )
                            .as_str(),
                        );
                    }
                    false => {
                        where_clause.push_str(
                            format!(
                                " AND id IN (SELECT id FROM {} WHERE resource_id = ?)",
                                reference::TABLE_NAME
                            )
                            .as_str(),
                        );
                    }
                }
            }

            let query_count_str = format!("SELECT COUNT(*) FROM {} {}", TABLE_NAME, where_clause);
            let mut query_count_builder: QueryScalar<'_, Any, i64, AnyArguments> =
                sqlx::query_scalar(&query_count_str);

            let query_data_str = format!(
                "SELECT * FROM {} {} ORDER BY ts DESC LIMIT ? OFFSET ?",
                TABLE_NAME, where_clause
            );

            let mut query_data_builder: QueryAs<'_, Any, DbRule, AnyArguments> =
                sqlx::query_as::<_, DbRule>(&query_data_str);

            if let Some(name) = query.name {
                let name = format!("%{}%", name);
                query_count_builder = query_count_builder.bind(name.clone());
                query_data_builder = query_data_builder.bind(name);
            }
            if let Some(status) = query.status {
                let status: i32 = status.into();
                query_count_builder = query_count_builder.bind(status);
                query_data_builder = query_data_builder.bind(status);
            }
            if let Some(parent_id) = query.parent_id {
                query_count_builder = query_count_builder.bind(parent_id.clone());
                query_data_builder = query_data_builder.bind(parent_id);
            }
            if let Some(resource_id) = query.resource_id {
                query_count_builder = query_count_builder.bind(resource_id.clone());
                query_data_builder = query_data_builder.bind(resource_id);
            }

            let count = query_count_builder.fetch_one(POOL.get().unwrap()).await?;
            let db_rules = query_data_builder
                .bind(limit)
                .bind(offset)
                .fetch_all(POOL.get().unwrap())
                .await?;

            (count, db_rules)
        }
    };

    let rules = db_rules
        .into_iter()
        .map(|x| x.transfer())
        .collect::<Result<Vec<Rule>>>()?;

    Ok((count as usize, rules))
}

pub async fn update_status(id: &String, status: Status) -> Result<()> {
    sqlx::query(format!("UPDATE {} SET status = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(Into::<i32>::into(status))
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn update(id: &String, req: CreateUpdateRuleReq) -> Result<()> {
    let conf = serde_json::to_vec(&req.conf)?;
    sqlx::query(format!("UPDATE {} SET name = ?, conf = ? WHERE id = ?", TABLE_NAME).as_str())
        .bind(req.name)
        .bind(conf)
        .bind(id)
        .execute(POOL.get().unwrap())
        .await?;
    Ok(())
}

pub async fn delete_by_id(id: &String) -> HaliaResult<()> {
    super::delete_by_id(id, TABLE_NAME).await
}
