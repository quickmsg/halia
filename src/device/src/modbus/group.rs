use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{bail, Result};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::RwLock;
use types::device::{CreateGroupReq, CreatePointReq, ListPointResp};

use super::point::Point;

pub(crate) struct Group {
    pub id: u64,
    pub name: String,
    pub interval: u64,
    pub points: RwLock<Vec<Point>>,
    auto_increment_id: AtomicU64,
}

#[derive(Deserialize)]
struct UpdateConf {
    pub name: Option<String>,
    pub interval: Option<u64>,
}

impl Group {
    pub fn new(conf: &CreateGroupReq, id: u64) -> Self {
        Group {
            id,
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

    pub async fn create_points(&self, create_points: Vec<CreatePointReq>) -> Result<()> {
        let mut points = Vec::with_capacity(create_points.len());
        for conf in create_points {
            let id = self.auto_increment_id.fetch_add(1, Ordering::SeqCst);
            let point = Point::new(conf, id)?;
            points.push(point);
        }
        self.points.write().await.extend(points);
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
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == id)
        {
            Some(point) => point.update(update_point).await,
            None => bail!("未找到点位：{}。", id),
        }
    }

    pub async fn get_points_num(&self) -> usize {
        self.points.read().await.len()
    }

    pub async fn delete_points(&self, ids: Vec<u64>) -> Result<()> {
        self.points.write().await.retain(|p| !ids.contains(&p.id));
        Ok(())
    }
}
