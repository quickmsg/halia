use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence::Status,
};
use group::Group;
use message::MessageBatch;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    types::{EndpointDescription, StatusCode},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
    time,
};
use tracing::{debug, error};
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
    conf: Arc<RwLock<Conf>>,
    groups: Arc<RwLock<Vec<Group>>>,
    session: Arc<RwLock<Option<Arc<Session>>>>,
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

pub(crate) fn new(id: Uuid, req: CreateDeviceReq) -> HaliaResult<Box<dyn Device>> {
    let conf: Conf = serde_json::from_value(req.conf.clone())?;
    Ok(Box::new(OpcUa {
        id,
        name: req.name.clone(),
        on: Arc::new(AtomicBool::new(false)),
        err: Arc::new(AtomicBool::new(false)),
        conf: Arc::new(RwLock::new(conf)),
        groups: Arc::new(RwLock::new(vec![])),
        session: Arc::new(RwLock::new(None)),
        group_signal_tx: None,
    }))
}

impl OpcUa {
    async fn run(&self) {
        let conf = self.conf.clone();
        let session = self.session.clone();
        let on = self.on.clone();
        tokio::spawn(async move {
            let now_conf = conf.read().await;

            loop {
                match OpcUa::get_session(&now_conf).await {
                    Ok((s, handle)) => {
                        session.write().await.replace(s);
                        match handle.await {
                            Ok(status_code) => match status_code {
                                StatusCode::Good => {
                                    if !on.load(Ordering::SeqCst) {
                                        return;
                                    }
                                }
                                _ => {
                                    *session.write().await = None;
                                    // 设备关闭后会跳到这里
                                    debug!("here");
                                }
                            },
                            Err(_) => {
                                debug!("connect err :here");
                            }
                        }
                    }
                    Err(e) => {
                        // TODO select
                        error!("connect error :{e:?}");
                        time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
    }

    async fn get_session(conf: &Conf) -> Result<(Arc<Session>, JoinHandle<StatusCode>)> {
        let mut client = ClientBuilder::new()
            .application_name("test")
            .application_uri("aasda")
            .trust_server_certs(true)
            .session_retry_limit(3)
            .create_sample_keypair(true)
            .keep_alive_interval(Duration::from_millis(100))
            .client()
            .unwrap();

        let endpoint: EndpointDescription = EndpointDescription::from(conf.url.as_ref());

        let (session, event_loop) = match client
            .new_session_from_endpoint(endpoint, IdentityToken::Anonymous)
            .await
        {
            Ok((session, event_loop)) => (session, event_loop),
            Err(e) => bail!("connect error {e:?}"),
        };

        let handle = event_loop.spawn();
        session.wait_for_connection().await;
        Ok((session, handle))
    }
}

#[async_trait]
impl Device for OpcUa {
    fn get_id(&self) -> Uuid {
        self.id
    }

    async fn recover(&mut self, status: Status) -> HaliaResult<()> {
        todo!()
    }

    async fn get_info(&self) -> SearchDeviceItemResp {
        SearchDeviceItemResp {
            id: self.id,
            name: self.name.clone(),
            r#type: TYPE,
            on: self.on.load(Ordering::SeqCst),
            err: self.err.load(Ordering::SeqCst),
            rtt: 9999,
            conf: json!(&self.conf.read().await.clone()),
        }
    }

    async fn start(&mut self) {
        if self.on.load(Ordering::SeqCst) {
            return;
        } else {
            self.on.store(true, Ordering::SeqCst);
        }
        self.run().await;
        let (group_signal_tx, _) = broadcast::channel::<group::Command>(16);
        for group in self.groups.write().await.iter_mut() {
            group.run(self.session.clone(), group_signal_tx.subscribe());
        }
        self.group_signal_tx = Some(group_signal_tx);
    }

    async fn stop(&mut self) {
        if !self.on.load(Ordering::SeqCst) {
            return;
        } else {
            self.on.store(false, Ordering::SeqCst);
        }

        match self
            .group_signal_tx
            .as_ref()
            .unwrap()
            .send(group::Command::StopAll)
        {
            Ok(_) => {}
            Err(e) => {
                error!("send stop all command err :{}", e);
            }
        }

        match self
            .session
            .write()
            .await
            .as_ref()
            .unwrap()
            .disconnect()
            .await
        {
            Ok(_) => {
                debug!("session disconnect success");
            }
            Err(e) => {
                debug!("err code is :{}", e);
            }
        }
    }

    async fn update(&mut self, req: &UpdateDeviceReq) -> HaliaResult<()> {
        let new_conf: Conf = serde_json::from_value(req.conf.clone())?;
        if self.name != req.name {
            self.name = req.name.clone();
        }

        let mut conf = self.conf.write().await;
        if conf.url != new_conf.url {
            conf.url = new_conf.url;
            let _ = self
                .session
                .write()
                .await
                .as_ref()
                .unwrap()
                .disconnect()
                .await;
        }

        Ok(())
    }

    async fn create_group(&mut self, group_id: Uuid, req: &CreateGroupReq) -> HaliaResult<()> {
        match Group::new(group_id, &req) {
            Ok(group) => {
                if self.on.load(Ordering::SeqCst) {
                    group.run(
                        self.session.clone(),
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
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == group_id)
        {
            Some(group) => {
                if group.interval != req.interval && self.on.load(Ordering::SeqCst) {
                    if let Err(e) = self
                        .group_signal_tx
                        .as_ref()
                        .unwrap()
                        .send(group::Command::Update(group_id, req.interval))
                    {
                        error!("group_signals send err :{}", e);
                    }
                }

                group.update(&req);
            }
            None => return Err(HaliaError::NotFound),
        };

        Ok(())
    }

    async fn delete_group(&self, group_id: Uuid) -> HaliaResult<()> {
        self.groups
            .write()
            .await
            .retain(|group| group_id != group.id);

        if self.on.load(Ordering::SeqCst) {
            match self
                .group_signal_tx
                .as_ref()
                .unwrap()
                .send(group::Command::Stop(group_id))
            {
                Ok(_) => {}
                Err(e) => error!("group send stop singla err:{}", e),
            }
        }

        Ok(())
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
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => Ok(group.search_points(page, size).await),
            None => Err(HaliaError::NotFound),
        }
    }

    async fn update_point(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        req: &CreatePointReq,
    ) -> HaliaResult<()> {
        match self
            .groups
            .read()
            .await
            .iter()
            .find(|group| group.id == group_id)
        {
            Some(group) => group.update_point(point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn write_point_value(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        value: serde_json::Value,
    ) -> HaliaResult<()> {
        todo!()
        // match self
        //     .groups
        //     .read()
        //     .await
        //     .iter()
        //     .find(|group| group.id == group_id)
        // {
        //     Some(group) => group.update_point(point_id, req).await,
        //     None => {
        //         debug!("未找到组");
        //         Err(HaliaError::NotFound)
        //     }
        // }
    }

    async fn delete_points(&self, group_id: &Uuid, point_ids: &Vec<Uuid>) -> HaliaResult<()> {
        match self
            .groups
            .write()
            .await
            .iter_mut()
            .find(|group| group.id == *group_id)
        {
            Some(group) => Ok(group.delete_points(point_ids).await),
            None => Err(HaliaError::NotFound),
        }
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

    async fn add_subscription(&self, req: Bytes) -> HaliaResult<()> {
        // TODO
        Err(HaliaError::ParseErr)
    }
}
