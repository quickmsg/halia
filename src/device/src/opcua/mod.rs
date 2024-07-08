use async_trait::async_trait;
use bytes::Bytes;
use common::error::{HaliaError, HaliaResult};
use group::Group;
use message::MessageBatch;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    types::EndpointDescription,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use types::device::{
    device::{CreateDeviceReq, SearchDeviceItemResp, SearchSinksResp, UpdateDeviceReq},
    group::{CreateGroupReq, SearchGroupItemResp, SearchGroupResp, UpdateGroupReq},
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
    session: Option<Arc<Session>>,
    group_signal_tx: Option<broadcast::Sender<group::Command>>,
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
        session: None,
        group_signal_tx: None,
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
        opcua::console_logging::init();
        let mut client = ClientBuilder::new()
            .application_name("test")
            .application_uri("aasda")
            .session_retry_limit(3)
            .client()
            .unwrap();

        let endpoint: EndpointDescription =
            EndpointDescription::from(self.conf.lock().await.url.as_ref());

        let (session, event_loop) = match client
            .new_session_from_endpoint(endpoint, IdentityToken::Anonymous)
            .await
        {
            Ok((session, event_loop)) => (session, event_loop),
            Err(e) => return Err(common::error::HaliaError::DeviceDisconnect),
        };

        let (group_signal_tx, _) = broadcast::channel::<group::Command>(16);
        self.group_signal_tx = Some(group_signal_tx);

        tokio::spawn(event_loop.run());
        session.wait_for_connection().await;
        let session_clone = session.clone();
        self.session = Some(session);

        for group in self.groups.write().await.iter_mut() {
            group.run(
                session_clone.clone(),
                self.group_signal_tx.as_ref().unwrap().subscribe(),
            );
        }

        Ok(())
    }

    async fn stop(&mut self) {
        todo!()
    }

    async fn update(&mut self, req: &UpdateDeviceReq) -> HaliaResult<()> {
        todo!()
    }

    async fn create_group(&mut self, group_id: Uuid, req: &CreateGroupReq) -> HaliaResult<()> {
        match Group::new(group_id, &req) {
            Ok(group) => {
                if self.on.load(Ordering::SeqCst) {
                    group.run(
                        self.session.as_ref().unwrap().clone(),
                        self.group_signal_tx.as_ref().unwrap().subscribe(),
                    );
                }

                self.groups.write().await.push(group);

                Ok(())
            }
            Err(_) => todo!(),
        }
    }

    async fn search_groups(&self, page: usize, size: usize) -> HaliaResult<SearchGroupResp> {
        let mut resps = Vec::new();
        for group in self
            .groups
            .read()
            .await
            .iter()
            .rev()
            .skip(((page - 1) * size) as usize)
        {
            resps.push({
                SearchGroupItemResp {
                    id: group.id,
                    name: group.name.clone(),
                    interval: group.interval,
                    point_count: group.get_points_num().await as u8,
                    desc: group.desc.clone(),
                }
            });
            if resps.len() == size as usize {
                break;
            }
        }
        Ok(SearchGroupResp {
            total: self.groups.read().await.len(),
            data: resps,
        })
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
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.create_point(point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
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
