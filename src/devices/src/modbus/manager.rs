use std::sync::LazyLock;

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc};
use types::{
    devices::{
        modbus::{
            CreateUpdateModbusReq, CreateUpdatePointReq, CreateUpdateSinkReq, SearchPointsResp,
            SearchSinksResp,
        },
        SearchDevicesItemResp,
    },
    Pagination, Value,
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

    pub async fn search(&self, device_id: &Uuid) -> HaliaResult<SearchDevicesItemResp> {
        match self.devices.get(device_id) {
            Some(device) => Ok(device.search().await),
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

    pub async fn create_point(
        &self,
        device_id: Uuid,
        point_id: Option<Uuid>,
        req: CreateUpdatePointReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_point(point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_points(
        &self,
        device_id: Uuid,
        pagination: Pagination,
    ) -> HaliaResult<SearchPointsResp> {
        match self.devices.get(&device_id) {
            Some(device) => Ok(device.search_points(pagination).await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_point(
        &self,
        device_id: Uuid,
        point_id: Uuid,
        req: CreateUpdatePointReq,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update_point(point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn write_point_value(
        &self,
        device_id: Uuid,
        point_id: Uuid,
        value: Value,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(device) => device.write_point_value(point_id, value).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_point(&self, device_id: Uuid, point_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.delete_point(point_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn add_point_ref(
        &self,
        device_id: &Uuid,
        point_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.add_point_ref(point_id, rule_id).await,
            None => Err(HaliaError::DeviceNotFound),
        }
    }

    pub async fn get_point_mb_rx(
        &self,
        device_id: &Uuid,
        point_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.get_point_mb_rx(point_id, rule_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn del_point_mb_rx(
        &self,
        device_id: &Uuid,
        point_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.del_point_mb_rx(point_id, rule_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn del_point_ref(
        &self,
        device_id: &Uuid,
        point_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(device_id) {
            Some(mut device) => device.del_point_ref(point_id, rule_id).await,
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

    pub async fn add_sink_ref(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.add_sink_ref(sink_id, rule_id),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn get_sink_mb_tx(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.get_sink_mb_tx(sink_id, rule_id),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn del_sink_mb_tx(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.del_sink_mb_tx(sink_id, rule_id),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn del_sink_ref(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.del_sink_ref(sink_id, rule_id),
            None => Err(HaliaError::NotFound),
        }
    }
}
