use common::error::{HaliaError, Result};
use tokio::sync::RwLock;
use types::device::{CreateGroupReq, CreatePointReq, ListPointResp, UpdateGroupReq};
use uuid::Uuid;

use crate::storage;

use super::point::Point;

#[derive(Debug)]
pub(crate) struct Group {
    pub id: Uuid,
    pub name: String,
    pub interval: u64,
    pub points: RwLock<Vec<(Uuid, Point)>>,
    pub device_id: Uuid,
}

impl Group {
    pub fn new(device_id: Uuid, group_id: Uuid, conf: &CreateGroupReq) -> Self {
        Group {
            id: group_id,
            device_id,
            name: conf.name.clone(),
            interval: conf.interval,
            points: RwLock::new(vec![]),
        }
    }

    pub fn update(&mut self, req: &UpdateGroupReq) {
        self.name = req.name.clone();
        self.interval = req.interval;
    }

    pub async fn create_points(
        &self,
        create_points: Vec<(Option<Uuid>, CreatePointReq)>,
    ) -> Result<()> {
        let mut points = Vec::with_capacity(create_points.len());
        let mut storage_infos = Vec::with_capacity(create_points.len());
        for (point_id, conf) in create_points {
            match point_id {
                Some(point_id) => {
                    let point = Point::new(conf.clone(), point_id)?;
                    points.push((point_id, point));
                }
                None => {
                    let point_id = Uuid::new_v4();
                    let point = Point::new(conf.clone(), point_id)?;
                    points.push((point_id, point));
                    storage_infos.push((point_id, serde_json::to_string(&conf)?));
                }
            }
        }

        self.points.write().await.extend(points);
        if storage_infos.len() > 0 {
            storage::insert_points(self.device_id, self.id, &storage_infos).await?;
        }
        Ok(())
    }

    pub async fn read_points(&self) -> Vec<ListPointResp> {
        let mut resps = Vec::with_capacity(self.get_points_num().await);
        for (_, point) in self.points.read().await.iter() {
            resps.push(ListPointResp {
                id: point.id,
                name: point.name.clone(),
                address: point.conf.address,
                r#type: "int16".to_string(),
                value: point.value.clone(),
                describe: point.conf.describe.clone(),
            })
        }

        resps
    }

    pub async fn update_point(&self, point_id: Uuid, req: &CreatePointReq) -> Result<()> {
        match self
            .points
            .write()
            .await
            .iter_mut()
            .find(|(id, _)| *id == point_id)
        {
            Some((_, point)) => point.update(req).await?,
            None => return Err(HaliaError::NotFound),
        };

        storage::update_point(
            self.device_id,
            self.id,
            point_id,
            serde_json::to_string(req)?,
        )
        .await?;

        Ok(())
    }

    pub async fn get_points_num(&self) -> usize {
        self.points.read().await.len()
    }

    pub async fn delete_points(&self, ids: Vec<Uuid>) -> Result<()> {
        self.points
            .write()
            .await
            .retain(|(id, _)| !ids.contains(id));
        storage::delete_points(self.device_id, self.id, &ids).await?;
        Ok(())
    }

    pub async fn set_data(&self, data: Vec<(Uuid, Vec<u8>)>) -> Result<()> {
        Ok(())
    }
}
