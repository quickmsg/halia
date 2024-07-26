use std::sync::LazyLock;

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc};
use types::{
    devices::{
        coap::{
            CreateUpdateAPIReq, CreateUpdateCoapReq, CreateUpdateSinkReq, SearchAPIsResp,
            SearchSinksResp,
        },
        SearchDevicesItemResp,
    },
    Pagination,
};
use uuid::Uuid;

use crate::GLOBAL_DEVICE_MANAGER;

use super::{Coap, TYPE};

pub static GLOBAL_COAP_MANAGER: LazyLock<Manager> = LazyLock::new(|| Manager {
    devices: DashMap::new(),
});

pub struct Manager {
    devices: DashMap<Uuid, Coap>,
}

impl Manager {
    pub async fn create(
        &self,
        device_id: Option<Uuid>,
        req: CreateUpdateCoapReq,
    ) -> HaliaResult<()> {
        let device = Coap::new(device_id, req).await?;
        GLOBAL_DEVICE_MANAGER.create(&TYPE, device.id).await;
        self.devices.insert(device.id.clone(), device);
        Ok(())
    }

    pub async fn recover(&self, device_id: &Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.recover().await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub fn search(&self, device_id: &Uuid) -> HaliaResult<SearchDevicesItemResp> {
        match self.devices.get(device_id) {
            Some(device) => Ok(device.search()),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update(&self, device_id: Uuid, req: CreateUpdateCoapReq) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update(req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn start(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.start().await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn stop(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.stop().await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => {
                device.delete().await?;
            }
            None => return Err(HaliaError::NotFound),
        };

        self.devices.remove(&device_id);
        GLOBAL_DEVICE_MANAGER.delete(&device_id).await;

        Ok(())
    }

    pub async fn create_api(
        &self,
        device_id: Uuid,
        api_id: Option<Uuid>,
        req: CreateUpdateAPIReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_api(api_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_apis(
        &self,
        device_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchAPIsResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.search_apis(pagination).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_api(
        &self,
        device_id: Uuid,
        api_id: Uuid,
        req: CreateUpdateAPIReq,
    ) -> HaliaResult<()> {
        match self.devices.get(&device_id) {
            Some(device) => device.update_api(api_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_api(&self, device_id: Uuid, api_id: Uuid) -> HaliaResult<()> {
        match self.devices.get(&device_id) {
            Some(device) => device.delete_api(api_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn subscribe(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.subscribe(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn unsubscribe(&self, device_id: &Uuid, group_id: &Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.unsubscribe(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_sink(
        &self,
        device_id: Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_sink(sink_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_sinks(
        &self,
        device_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchSinksResp> {
        match self.devices.get(&device_id) {
            Some(device) => Ok(device.search_sinks(pagination).await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_sink(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update_sink(sink_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_sink(&self, device_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete_sink(sink_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn publish(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.publish(sink_id).await,
            None => Err(HaliaError::NotFound),
        }
    }
}
