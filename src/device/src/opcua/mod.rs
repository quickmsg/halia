use async_trait::async_trait;
use bytes::Bytes;
use common::error::HaliaResult;
use group::Group;
use message::MessageBatch;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use types::device::{
    device::{CreateDeviceReq, SearchDeviceItemResp, SearchSinksResp, UpdateDeviceReq},
    group::{CreateGroupReq, SearchGroupResp, UpdateGroupReq},
    point::{CreatePointReq, SearchPointResp},
};
use uuid::Uuid;

use crate::Device;

pub(crate) const TYPE: &str = "opcua";
mod group;
mod point;

struct OpcUa {
    id: Uuid,
    name: String,
    on: Arc<AtomicBool>,
    err: Arc<AtomicBool>,
    conf: Arc<Mutex<Conf>>,
    groups: Arc<RwLock<Vec<Group>>>,
}

#[derive(Deserialize, Serialize, Clone)]
struct Conf {
    url: String,
}

struct Password {
    username: String,
    password: String,
}

pub(crate) fn new(id: Uuid, req: &CreateDeviceReq) -> HaliaResult<Box<dyn Device>> {
    let conf: Conf = serde_json::from_value(req.conf.clone())?;

    Ok(Box::new(OpcUa {
        id,
        name: req.name.clone(),
        on: Arc::new(AtomicBool::new(false)),
        err: Arc::new(AtomicBool::new(false)),
        conf: Arc::new(Mutex::new(conf)),
        groups: Arc::new(RwLock::new(vec![])),
    }))
}

#[async_trait]
impl Device for OpcUa {
    fn get_id(&self) -> Uuid {
        self.id
    }

    async fn get_info(&self) -> SearchDeviceItemResp {
        SearchDeviceItemResp {
            id: self.id,
            name: self.name.clone(),
            r#type: TYPE,
            on: self.on.load(Ordering::SeqCst),
            err: self.err.load(Ordering::SeqCst),
            rtt: 9999,
            conf: json!(&self.conf.lock().await.clone()),
        }
    }

    async fn start(&mut self) -> HaliaResult<()> {
        todo!()
    }

    async fn stop(&mut self) {
        todo!()
    }

    async fn update(&mut self, req: &UpdateDeviceReq) -> HaliaResult<()> {
        todo!()
    }

    // group
    async fn create_group(&mut self, group_id: Uuid, req: &CreateGroupReq) -> HaliaResult<()> {
        match Group::new(group_id, &req) {
            Ok(_) => todo!(),
            Err(_) => todo!(),
        }
    }

    async fn search_groups(&self, page: usize, size: usize) -> HaliaResult<SearchGroupResp> {
        todo!()
    }

    async fn update_group(&self, group_id: Uuid, req: UpdateGroupReq) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_group(&self, group_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    // points
    async fn create_point(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        req: CreatePointReq,
    ) -> HaliaResult<()> {
        todo!()
    }

    async fn search_point(
        &self,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchPointResp> {
        todo!()
    }

    async fn update_point(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        req: &CreatePointReq,
    ) -> HaliaResult<()> {
        todo!()
    }

    async fn write_point_value(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        value: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_points(&self, group_id: &Uuid, point_ids: &Vec<Uuid>) -> HaliaResult<()> {
        todo!()
    }

    async fn subscribe(
        &mut self,
        group_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        todo!()
    }

    async fn unsubscribe(&mut self, group_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn create_sink(&self, sink_id: Uuid, req: &Bytes) -> HaliaResult<()> {
        todo!()
    }

    async fn search_sinks(&self, page: usize, size: usize) -> SearchSinksResp {
        todo!()
    }

    async fn update_sink(&self, sink_id: Uuid, req: &Bytes) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_sink(&self, sink_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn publish(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        todo!()
    }
}
