use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{bail, Result};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::RwLock;
use types::device::{CreateGroupReq, CreatePointReq, ListPointResp};

use crate::storage;

use super::point::Point;

pub(crate) struct Group {
    pub id: u64,
    pub name: String,
    pub interval: u64,
    pub points: RwLock<Vec<Point>>,
    pub device_id: u64,
    auto_increment_id: AtomicU64,
}

#[derive(Deserialize)]
struct UpdateConf {
    pub name: Option<String>,
    pub interval: Option<u64>,
}

impl Group {
    pub fn new(device_id: u64, group_id: u64, conf: &CreateGroupReq) -> Self {
        Group {
            id: group_id,
            device_id,
            name: conf.name.clone(),
            interval: conf.interval,
            points: RwLock::new(vec![]),
            auto_increment_id: AtomicU64::new(1),
        }
    }

    pub fn update(&mut self, update_group: Value) -> Result<bool> {
        let update_conf: UpdateConf = serde_json::from_value(update_group)?;
        if let Some(name) = update_conf.name {
            self.name = name;
        }

        let mut change_interval = false;
        if let Some(interval) = update_conf.interval {
            self.interval = interval;
            change_interval = true;
        }

        Ok(change_interval)
    }

    pub async fn create_points(
        &self,
        create_points: Vec<(Option<u64>, CreatePointReq)>,
    ) -> Result<()> {
        let mut points = Vec::with_capacity(create_points.len());
        let mut storage_infos = Vec::with_capacity(create_points.len());
        for (id, conf) in create_points {
            let id = self.auto_increment_id.fetch_add(1, Ordering::SeqCst);
            let point = Point::new(conf.clone(), id)?;
            points.push(point);
            storage_infos.push((id, serde_json::to_string(&conf)?))
        }
        self.points.write().await.extend(points);
        storage::insert_points(self.device_id, self.id, &storage_infos).await?;
        Ok(())
    }

    pub async fn insert_points(&self, points: Vec<(u64, CreatePointReq)>) -> Result<()> {
        Ok(())
    }

    pub async fn read_points(&self) -> Vec<ListPointResp> {
        let mut resps = Vec::with_capacity(self.get_points_num().await);
        for point in self.points.read().await.iter() {
            resps.push(ListPointResp {
                id: point.id,
                name: point.name.clone(),
                address: point.address,
                r#type: "int16".to_string(),
                value: point.value.clone(),
                attribute: 3,
                multi: 1.5,
                describe: point.describe.clone(),
            })
        }

        resps
    }

    pub async fn update_point(&self, id: u64, update_point: Value) -> Result<()> {
        let update_data = serde_json::to_string(&update_point)?;

        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == id)
        {
            Some(point) => point.update(update_point).await?,
            None => bail!("未找到点位：{}。", id),
        };

        storage::update_point(self.device_id, self.id, id, update_data).await?;

        Ok(())
    }

    pub async fn get_points_num(&self) -> usize {
        self.points.read().await.len()
    }

    pub async fn delete_points(&self, ids: Vec<u64>) -> Result<()> {
        self.points.write().await.retain(|p| !ids.contains(&p.id));
        storage::delete_points(self.device_id, self.id, &ids).await?;
        Ok(())
    }
}
