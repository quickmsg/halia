use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use group::Group;
use message::MessageBatch;
use protocol::coap::client::UdpCoAPClient;
use serde_json::json;
use sink::Sink;
use std::{str::FromStr, sync::Arc};
use tokio::sync::{broadcast, mpsc};
use types::devices::{
    coap::{
        CreateUpdateCoapReq, CreateUpdateGroupAPIReq, CreateUpdateGroupReq, CreateUpdateSinkReq,
        SearchGroupAPIsResp, SearchGroupsResp, SearchSinksResp,
    },
    SearchDevicesItemResp,
};
use uuid::Uuid;

pub const TYPE: &str = "coap";
mod group;
mod group_api;
pub mod manager;
mod sink;

pub struct Coap {
    id: Uuid,
    conf: CreateUpdateCoapReq,
    client: Option<Arc<UdpCoAPClient>>,
    groups: Vec<Group>,
    sinks: Vec<Sink>,
}

impl Coap {
    pub async fn new(device_id: Option<Uuid>, req: CreateUpdateCoapReq) -> HaliaResult<Self> {
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::coap::create(
                &device_id,
                TYPE,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Coap {
            id: device_id,
            conf: req,
            client: None,
            groups: vec![],
            sinks: vec![],
        })
    }

    async fn recover(&mut self) -> HaliaResult<()> {
        match persistence::devices::coap::read_groups(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);

                    let group_id = Uuid::from_str(items[0]).unwrap();
                    let req: CreateUpdateGroupReq = serde_json::from_str(items[1])?;
                    self.create_group(Some(group_id), req).await?;
                }

                for group in self.groups.iter_mut() {
                    group.recover(&self.id).await?;
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn search(&self) -> SearchDevicesItemResp {
        // TODO
        SearchDevicesItemResp {
            id: self.id.clone(),
            r#type: TYPE,
            on: self.client.is_some(),
            err: false,
            rtt: 999,
            conf: json!(&self.conf),
        }
    }

    pub async fn update(&mut self, req: CreateUpdateCoapReq) -> HaliaResult<()> {
        persistence::devices::update_device_conf(&self.id, serde_json::to_string(&req).unwrap())
            .await?;

        let mut restart = false;
        if self.conf.host != req.host || self.conf.port != req.port {
            restart = true;
        }

        self.conf = req;

        if restart && self.client.is_some() {
            // restart
            todo!()
        }

        Ok(())
    }

    pub async fn start(&mut self) -> HaliaResult<()> {
        if self.client.is_some() {
            return Ok(());
        }

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;

        let client = UdpCoAPClient::new_udp((self.conf.host.clone(), self.conf.port)).await?;
        let clone_client = Arc::new(client);
        for group in self.groups.iter_mut() {
            group.start(clone_client.clone());
        }
        self.client = Some(clone_client);

        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if self.client.is_none() {
            return Ok(());
        }
        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;
        for group in self.groups.iter_mut() {
            group.stop().await;
        }
        self.client = None;

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if self.client.is_some() {
            return Err(HaliaError::DeviceRunning);
        }

        persistence::devices::delete_device(&self.id).await?;

        Ok(())
    }

    pub async fn create_group(
        &mut self,
        group_id: Option<Uuid>,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match Group::new(&self.id, group_id, req).await {
            Ok(mut group) => {
                match &self.client {
                    Some(client) => group.start(client.clone()),
                    None => {}
                }
                self.groups.push(group);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn search_groups(&self, page: usize, size: usize) -> SearchGroupsResp {
        let mut data = vec![];
        for group in self.groups.iter().rev().skip((page - 1) * size) {
            data.push(group.search());
            if data.len() == size {
                break;
            }
        }

        SearchGroupsResp {
            total: self.groups.len(),
            data,
        }
    }

    pub async fn update_group(
        &mut self,
        group_id: Uuid,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match self.groups.iter_mut().find(|group| group.id == group_id) {
            Some(group) => group.update(&self.id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_group(&mut self, group_id: Uuid) -> HaliaResult<()> {
        match self.groups.iter_mut().find(|group| group.id == group_id) {
            Some(group) => match group.delete(&self.id).await {
                Ok(_) => {
                    self.groups.retain(|group| group.id != group_id);
                    Ok(())
                }
                Err(e) => Err(e),
            },
            None => return Err(HaliaError::NotFound),
        }
    }

    pub async fn create_group_api(
        &mut self,
        group_id: Uuid,
        api_id: Option<Uuid>,
        req: CreateUpdateGroupAPIReq,
    ) -> HaliaResult<()> {
        match self.groups.iter_mut().find(|group| group.id == group_id) {
            Some(group) => group.create_api(&self.id, api_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_group_apis(
        &self,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupAPIsResp> {
        match self.groups.iter().find(|group| group.id == group_id) {
            Some(group) => Ok(group.search_apis(page, size).await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_group_api(
        &self,
        group_id: Uuid,
        resource_id: Uuid,
        req: CreateUpdateGroupAPIReq,
    ) -> HaliaResult<()> {
        match self.groups.iter().find(|group| group.id == group_id) {
            Some(group) => group.update_api(&self.id, resource_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_group_apis(&self, group_id: Uuid, api_ids: Vec<Uuid>) -> HaliaResult<()> {
        match self.groups.iter().find(|group| group.id == group_id) {
            Some(group) => group.delete_apis(&self.id, api_ids).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn subscribe(
        &self,
        group_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        todo!()
    }

    pub async fn unsubscribe(&self, group_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(&self.id, sink_id, req).await {
            Ok(sink) => Ok(self.sinks.push(sink)),
            Err(e) => Err(e),
        }
    }

    pub async fn search_sinks(&self, page: usize, size: usize) -> SearchSinksResp {
        let mut data = vec![];
        for sink in self.sinks.iter().rev().skip((page - 1) * size) {
            data.push(sink.search());
            if data.len() == size {
                break;
            }
        }

        SearchSinksResp {
            total: self.sinks.len(),
            data,
        }
    }

    pub async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => sink.update(&self.id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id != sink_id);
                Ok(())
            }
            None => todo!(),
        }
    }

    pub async fn publish(&self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        todo!()
    }
}
