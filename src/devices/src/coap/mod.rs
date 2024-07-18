use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use group::Group;
use protocol::coap::client::UdpCoAPClient;
use serde_json::json;
use std::sync::Arc;
use types::devices::{
    coap::{
        CreateUpdateCoapReq, CreateUpdateGroupReq, CreateUpdateGroupResourceReq,
        SearchGroupResourcesResp, SearchGroupsResp,
    },
    SearchDevicesItemResp,
};
use uuid::Uuid;

pub const TYPE: &str = "coap";
mod group;
pub mod manager;
mod resource;
mod sink;

pub struct Coap {
    id: Uuid,
    conf: CreateUpdateCoapReq,
    client: Option<Arc<UdpCoAPClient>>,
    groups: Vec<Group>,
}

impl Coap {
    pub async fn new(device_id: Option<Uuid>, req: CreateUpdateCoapReq) -> HaliaResult<Self> {
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };

        if new {
            persistence::devices::coap::create(&device_id, serde_json::to_string(&req).unwrap())
                .await?;
        }

        Ok(Coap {
            id: device_id,
            conf: req,
            client: None,
            groups: vec![],
        })
    }

    async fn recover(&mut self) -> HaliaResult<()> {
        // let paths = persistence::device::read_coap_paths(&self.id).await?;
        // for (id, data) in paths {
        //     let path = path::new(id, &data).await?;
        //     self.paths.write().await.push(path);
        // }
        // if status == Status::Runing {
        //     self.start().await;
        // }
        Ok(())
    }

    pub fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id.clone(),
            r#type: TYPE,
            on: todo!(),
            err: todo!(),
            rtt: todo!(),
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

    pub async fn create_group_resource(
        &mut self,
        group_id: Uuid,
        resource_id: Option<Uuid>,
        req: CreateUpdateGroupResourceReq,
    ) -> HaliaResult<()> {
        match self.groups.iter_mut().find(|group| group.id == group_id) {
            Some(group) => group.create_resource(&self.id, resource_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_group_resources(
        &self,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupResourcesResp> {
        match self.groups.iter().find(|group| group.id == group_id) {
            Some(group) => Ok(group.search_resources(page, size).await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_group_resource(
        &self,
        group_id: Uuid,
        resource_id: Uuid,
        req: CreateUpdateGroupResourceReq,
    ) -> HaliaResult<()> {
        match self.groups.iter().find(|group| group.id == group_id) {
            Some(group) => group.update_resource(&self.id, resource_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_group_resources(
        &self,
        group_id: Uuid,
        resource_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        match self.groups.iter().find(|group| group.id == group_id) {
            Some(group) => group.delete_resources(&self.id, resource_ids).await,
            None => todo!(),
        }
    }
}
