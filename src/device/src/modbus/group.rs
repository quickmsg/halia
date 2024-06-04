use std::sync::atomic::{AtomicU64, Ordering};

use common::error::{HaliaError, Result};
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::debug;
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

    pub fn update(&mut self, req: &CreateGroupReq) -> Result<()> {
        self.name = req.name.clone();
        self.interval = req.interval;
        Ok(())
    }

    pub async fn create_points(
        &self,
        create_points: Vec<(Option<u64>, CreatePointReq)>,
    ) -> Result<()> {
        debug!("create points");
        let mut points = Vec::with_capacity(create_points.len());
        let mut storage_infos = Vec::with_capacity(create_points.len());
        for (point_id, conf) in create_points {
            match point_id {
                Some(point_id) => {
                    let point = Point::new(conf.clone(), point_id)?;
                    points.push(point);
                }
                None => {
                    let point_id = self.auto_increment_id.fetch_add(1, Ordering::SeqCst);
                    let point = Point::new(conf.clone(), point_id)?;
                    points.push(point);
                    storage_infos.push((point_id, serde_json::to_string(&conf)?));
                }
            }
        }

        self.points.write().await.extend(points);
        if storage_infos.len() > 0 {
            debug!("stroage info length > 0");
            storage::insert_points(self.device_id, self.id, &storage_infos).await?;
        }
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

    pub async fn update_point(&self, id: u64, req: &CreatePointReq) -> Result<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|point| point.id == id)
        {
            Some(point) => point.update(req).await?,
            None => return Err(HaliaError::NotFound),
        };

        storage::update_point(self.device_id, self.id, id, serde_json::to_string(req)?).await?;

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
