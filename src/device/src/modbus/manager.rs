use std::sync::LazyLock;

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc};
use types::devices::{
    modbus::{
        CreateUpdateGroupPointReq, CreateUpdateGroupReq, CreateUpdateModbusReq, CreateUpdateSinkPointReq, CreateUpdateSinkReq, SearchGroupPointsResp, SearchGroupsResp, SearchSinkPointsResp, SearchSinksResp
    },
    SearchDevicesItemResp,
};
use uuid::Uuid;

use crate::GLOBAL_DEVICE_MANAGER;

use super::{Modbus, TYPE};

pub static GLOBAL_MODBUS_MANAGER: LazyLock<Manager> = LazyLock::new(|| Manager {
    devices: DashMap::new(),
});

pub struct Manager {
    devices: DashMap<Uuid, Modbus>,
}

impl Manager {
    pub async fn create(
        &self,
        device_id: Option<Uuid>,
        req: CreateUpdateModbusReq,
    ) -> HaliaResult<()> {
        let device = Modbus::new(device_id, req).await?;
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

    pub async fn update(&self, device_id: Uuid, req: CreateUpdateModbusReq) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update(req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn start(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.start(true).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn stop(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.stop(true).await,
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

    pub async fn create_group(
        &self,
        device_id: Uuid,
        group_id: Option<Uuid>,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_group(group_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_groups(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupsResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.search_groups(page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_group(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        req: CreateUpdateGroupReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update_group(group_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_group(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete_group(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_group_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Option<Uuid>,
        req: CreateUpdateGroupPointReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_group_point(group_id, point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_group_points(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupPointsResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.search_group_points(group_id, page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_group_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Uuid,
        req: CreateUpdateGroupPointReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update_group_point(group_id, point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn write_group_point_value(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(device) => device.write_point_value(group_id, point_id, data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_group_points(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete_group_points(group_id, point_ids).await,
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
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSinksResp> {
        match self.devices.get(&device_id) {
            Some(device) => Ok(device.search_sinks(page, size).await),
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

    pub async fn create_sink_point(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        point_id: Option<Uuid>,
        req: CreateUpdateSinkPointReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_sink_point(sink_id, point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_sink_points(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSinkPointsResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.search_sink_points(sink_id, page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_sink_point(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        point_id: Uuid,
        req: CreateUpdateSinkPointReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update_sink_point(sink_id, point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_sink_points(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        point_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete_sink_points(sink_id, point_ids).await,
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
