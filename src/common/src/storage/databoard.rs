use anyhow::Result;
use sqlx::{prelude::FromRow, AnyPool};
use tracing::debug;
use uuid::Uuid;

#[derive(FromRow)]
pub struct Databoard {
    pub id: String,
    pub conf: String,
}

#[derive(FromRow)]
pub struct DataboardData {
    pub id: String,
    pub conf: String,
}

pub async fn create_databoard(pool: &AnyPool, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("INSERT INTO databoards (id, conf) VALUES (?1, ?2)")
        .bind(id.to_string())
        .bind(conf)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn read_databoards(pool: &AnyPool) -> Result<Vec<Databoard>> {
    let databoards = sqlx::query_as::<_, Databoard>("SELECT id, conf FROM databoards")
        .fetch_all(pool)
        .await?;

    // let mut databoards = vec![];
    // for row in rows {
    //     let id: String = row.get("id");
    //     let conf: String = row.get("conf");
    //     databoards.push(Databoard { id, conf })
    // }

    Ok(databoards)
}

pub async fn update_databoard(pool: &AnyPool, id: &Uuid, conf: String) -> Result<()> {
    sqlx::query("UPDATE databoards SET conf = ?1 WHERE id = ?2")
        .bind(id.to_string())
        .bind(conf)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_databoard(pool: &AnyPool, id: &Uuid) -> Result<()> {
    sqlx::query(
        r#"
DELETE FROM databoards WHERE id = ?1;
DELETE FROM databoard_datas WHERE parent_id = ?1;
    "#,
    )
    .bind(id.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn create_databoard_data(
    pool: &AnyPool,
    databoard_id: &Uuid,
    databoard_data_id: &Uuid,
    conf: String,
) -> Result<()> {
    sqlx::query("INSERT INTO databoard_datas (id, parent_id, conf) VALUES (?1, ?2, ?3)")
        .bind(databoard_data_id.to_string())
        .bind(databoard_id.to_string())
        .bind(conf)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn read_databoard_datas(
    pool: &AnyPool,
    databoard_id: &Uuid,
) -> Result<Vec<DataboardData>> {
    debug!("{}", databoard_id);
    let databoard_datas = sqlx::query_as::<_, DataboardData>(
        "SELECT id, conf FROM databoard_datas WHERE parent_id = ?1",
    )
    .bind(databoard_id.to_string())
    .fetch_all(pool)
    .await?;

    // let mut databoard_datas = vec![];
    // for row in rows {
    //     let id: String = row.get("id");
    //     let conf: String = row.get("conf");
    //     databoard_datas.push(DataboardData { id, conf })
    // }

    Ok(databoard_datas)
}

pub async fn update_databoard_data(
    pool: &AnyPool,
    databoard_data_id: &Uuid,
    conf: String,
) -> Result<()> {
    sqlx::query("UPDATE databoard_datas SET conf = ?1 WHERE id = ?2")
        .bind(conf)
        .bind(databoard_data_id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_databoard_data(pool: &AnyPool, databoard_data_id: &Uuid) -> Result<()> {
    sqlx::query("DELETE FROM databoard_datas WHERE id = ?1")
        .bind(databoard_data_id.to_string())
        .execute(pool)
        .await?;
    Ok(())
}
