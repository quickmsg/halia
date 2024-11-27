use anyhow::Result;
use sqlx::prelude::FromRow;
use tracing::debug;
use types::{RuleRefCnt, Status};

use super::POOL;

pub(crate) static TABLE_NAME: &str = "rule_refs";

#[derive(FromRow)]
pub struct DbRuleRef {
    pub rule_id: String,
    pub parent_id: String,
    pub resource_id: String,
    pub status: i32,
}

impl TryFrom<DbRuleRef> for RuleRef {
    type Error = anyhow::Error;

    fn try_from(value: DbRuleRef) -> Result<Self> {
        Ok(RuleRef {
            rule_id: value.rule_id,
            parent_id: value.parent_id,
            resource_id: value.resource_id,
            status: Status::try_from(value.status)?,
        })
    }
}

pub struct RuleRef {
    pub rule_id: String,
    pub parent_id: String,
    pub resource_id: String,
    pub status: Status,
}

pub(crate) fn create_table() -> String {
    format!(
        r#"  
CREATE TABLE IF NOT EXISTS {} (
    rule_id CHAR(32) NOT NULL,
    parent_id CHAR(32) NOT NULL,
    resource_id CHAR(32) NOT NULL,
    status SMALLINT NOT NULL
);
"#,
        TABLE_NAME
    )
}

pub async fn insert(rule_id: &String, parent_id: &String, resource_id: &String) -> Result<()> {
    sqlx::query(
        format!(
            "INSERT INTO {} (rule_id, parent_id, resource_id, status) VALUES (?, ?, ?, ?)",
            TABLE_NAME
        )
        .as_str(),
    )
    .bind(rule_id)
    .bind(parent_id)
    .bind(resource_id)
    .bind(Into::<i32>::into(Status::Stopped))
    .execute(POOL.get().unwrap())
    .await?;
    Ok(())
}

pub async fn update_status_by_rule_id(rule_id: &String, status: Status) -> Result<()> {
    sqlx::query("UPDATE rule_refs SET status = ? WHERE rule_id = ?")
        .bind(Into::<i32>::into(status))
        .bind(rule_id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn delete_many_by_rule_id(rule_id: &String) -> Result<()> {
    sqlx::query(format!("DELETE FROM {} WHERE rule_id = ?", TABLE_NAME).as_str())
        .bind(rule_id)
        .execute(POOL.get().unwrap())
        .await?;

    Ok(())
}

pub async fn count_cnt_by_parent_id(parent_id: &String, status: Option<Status>) -> Result<usize> {
    let cnt = match status {
        Some(status) => {
            let cnt: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE parent_id = ? AND status = ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(parent_id)
            .bind(Into::<i32>::into(status))
            .fetch_one(POOL.get().unwrap())
            .await?;
            cnt
        }
        None => {
            let cnt: i64 = sqlx::query_scalar(
                format!("SELECT COUNT(*) FROM {} WHERE parent_id = ?", TABLE_NAME).as_str(),
            )
            .bind(parent_id)
            .fetch_one(POOL.get().unwrap())
            .await?;
            cnt
        }
    };

    Ok(cnt as usize)
}

pub async fn get_rule_ref_info_by_parent_id(parent_id: &String) -> Result<RuleRefCnt> {
    Ok(RuleRefCnt {
        rule_reference_running_cnt: count_cnt_by_parent_id(parent_id, Some(Status::Running))
            .await?,
        rule_reference_total_cnt: count_cnt_by_parent_id(parent_id, None).await?,
    })
}

pub async fn count_cnt_by_resource_id(
    resource_id: &String,
    status: Option<Status>,
) -> Result<usize> {
    let cnt = match status {
        Some(status) => {
            let cnt: i64 = sqlx::query_scalar(
                format!(
                    "SELECT COUNT(*) FROM {} WHERE resource_id = ? AND status = ?",
                    TABLE_NAME
                )
                .as_str(),
            )
            .bind(resource_id)
            .bind(Into::<i32>::into(status))
            .fetch_one(POOL.get().unwrap())
            .await?;
            cnt
        }
        None => {
            let cnt: i64 = sqlx::query_scalar(
                format!("SELECT COUNT(*) FROM {} WHERE resource_id = ?", TABLE_NAME).as_str(),
            )
            .bind(resource_id)
            .fetch_one(POOL.get().unwrap())
            .await?;
            cnt
        }
    };

    Ok(cnt as usize)
}

pub async fn get_rule_ref_info_by_resource_id(resource_id: &String) -> Result<RuleRefCnt> {
    Ok(RuleRefCnt {
        rule_reference_running_cnt: count_cnt_by_resource_id(resource_id, Some(Status::Running))
            .await?,
        rule_reference_total_cnt: count_cnt_by_resource_id(resource_id, None).await?,
    })
}

pub async fn count_cnt_by_many_resource_ids(resource_ids: &Vec<String>) -> Result<usize> {
    let mut clause = format!("SELECT COUNT(*) FROM {} WHERE resource_id IN (", TABLE_NAME,);
    for resource_id in resource_ids {
        clause.push_str(format!("{},", resource_id).as_str());
    }
    clause.push_str(")");

    debug!("clause: {}", clause);

    let count: i64 = sqlx::query_scalar(clause.as_str())
        .fetch_one(POOL.get().unwrap())
        .await?;

    Ok(count as usize)
}

pub async fn count_active_cnt_by_resource_id(resource_id: &String) -> Result<usize> {
    let active_cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE active = ? AND resource_id = ?")
            .bind(true as i32)
            .bind(resource_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(active_cnt as usize)
}

pub async fn count_running_cnt_by_resource_id(resource_id: &String) -> Result<usize> {
    let active_cnt: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rule_refs WHERE active = ? AND resource_id = ?")
            .bind(true as i32)
            .bind(resource_id)
            .fetch_one(POOL.get().unwrap())
            .await?;

    Ok(active_cnt as usize)
}
