use std::sync::LazyLock;

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::broadcast;
use types::{
    devices::{
        coap::{
            CreateUpdateAPIReq, CreateUpdateCoapReq, CreateUpdateObserveReq, CreateUpdateSinkReq,
            SearchAPIsResp, SearchObservesResp, SearchSinksResp,
        },
        DeviceType, SearchDevicesItemResp,
    },
    Pagination,
};
use uuid::Uuid;

use crate::GLOBAL_DEVICE_MANAGER;

use super::Coap;

pub static GLOBAL_COAP_MANAGER: LazyLock<Manager> = LazyLock::new(|| Manager {
    devices: DashMap::new(),
});

pub struct Manager {
    devices: DashMap<Uuid, Coap>,
}

macro_rules! device_not_find_err {
    ($device_id:expr) => {
        Err(HaliaError::NotFound("coap设备".to_owned(), $device_id))
    };
}

impl Manager {
    pub async fn create(
        &self,
        device_id: Option<Uuid>,
        req: CreateUpdateCoapReq,
    ) -> HaliaResult<()> {
        GLOBAL_DEVICE_MANAGER.check_duplicate_name(&device_id, &req.base.name)?;

        let device = Coap::new(device_id, req).await?;
        GLOBAL_DEVICE_MANAGER
            .create(DeviceType::Coap, device.id)
            .await;
        self.devices.insert(device.id.clone(), device);
        Ok(())
    }

    pub fn check_duplicate_name(&self, device_id: &Option<Uuid>, name: &str) -> HaliaResult<()> {
        if self
            .devices
            .iter()
            .any(|device| !device.check_duplicate_name(device_id, name))
        {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub async fn recover(&self, device_id: &Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.recover().await,
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub fn search(&self, device_id: &Uuid) -> HaliaResult<SearchDevicesItemResp> {
        match self.devices.get(device_id) {
            Some(device) => Ok(device.search()),
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub async fn update(&self, device_id: Uuid, req: CreateUpdateCoapReq) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update(req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn start(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.start().await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn stop(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.stop().await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn delete(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => {
                device.delete().await?;
            }
            None => return device_not_find_err!(device_id),
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
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn search_apis(
        &self,
        device_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchAPIsResp> {
        match self.devices.get(&device_id) {
            Some(device) => Ok(device.search_apis(pagination).await),
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn update_api(
        &self,
        device_id: Uuid,
        api_id: Uuid,
        req: CreateUpdateAPIReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update_api(api_id, req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn delete_api(&self, device_id: Uuid, api_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete_api(api_id).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn add_api_ref(
        &self,
        device_id: &Uuid,
        api_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.add_api_ref(api_id, rule_id).await,
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub async fn get_api_mb_rx(
        &self,
        device_id: &Uuid,
        api_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.get_api_mb_rx(api_id, rule_id).await,
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub async fn del_api_ref(
        &self,
        device_id: &Uuid,
        api_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.del_api_ref(api_id, rule_id).await,
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub async fn del_api_mb_rx(
        &self,
        device_id: &Uuid,
        api_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.del_api_mb_rx(api_id, rule_id).await,
            None => device_not_find_err!(device_id.clone()),
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
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn search_sinks(
        &self,
        device_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchSinksResp> {
        match self.devices.get(&device_id) {
            Some(device) => Ok(device.search_sinks(pagination).await),
            None => device_not_find_err!(device_id),
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
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn delete_sink(&self, device_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete_sink(sink_id).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub fn add_sink_ref(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.add_sink_ref(sink_id, rule_id),
            None => device_not_find_err!(device_id.clone()),
        }
    }

    pub fn del_sink_ref(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.add_sink_ref(sink_id, rule_id),
            None => device_not_find_err!(device_id.clone()),
        }
    }
}

// observe
impl Manager {
    pub async fn create_observe(
        &self,
        device_id: Uuid,
        observe_id: Option<Uuid>,
        req: CreateUpdateObserveReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_observe(observe_id, req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn search_observes(
        &self,
        device_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchObservesResp> {
        match self.devices.get(&device_id) {
            Some(device) => Ok(device.search_observes(pagination).await),
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn update_observe(
        &self,
        device_id: Uuid,
        observe_id: Uuid,
        req: CreateUpdateObserveReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update_observe(observe_id, req).await,
            None => device_not_find_err!(device_id),
        }
    }

    pub async fn delete_observe(&self, device_id: Uuid, observe_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete_observe(observe_id).await,
            None => device_not_find_err!(device_id),
        }
    }
}
