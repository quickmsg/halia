use std::sync::LazyLock;

use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use types::device::{device::SearchSinksResp, group::SearchGroupResp, point::SearchPointResp};
use uuid::Uuid;

use super::Modbus;

pub static GLOBAL_MODBUS_MANAGER: LazyLock<Manager> = LazyLock::new(|| Manager {
    devices: DashMap::new(),
});

pub struct Manager {
    devices: DashMap<Uuid, Modbus>,
}

// for modbus
impl Manager {
    pub async fn create(&self, device_id: Option<Uuid>, data: String) -> HaliaResult<()> {
        let device = Modbus::new(device_id, &data).await?;
        self.devices.insert(device.id, device);
        Ok(())
    }

    pub async fn update(&self, device_id: Uuid, data: String) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update(data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn start(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => Ok(device.start().await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn stop(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => Ok(device.stop().await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => {
                todo!()
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_group(
        &self,
        device_id: Uuid,
        group_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_group(group_id, data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_groups(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.search_groups(page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_group(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(device) => device.update_group(group_id, data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_group(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(device) => device.delete_group(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_group_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(device) => device.create_group_point(group_id, point_id, data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_group_points(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchPointResp> {
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
        data: String,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(device) => device.update_group_point(group_id, point_id, data).await,
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

    pub async fn create_sink(
        &self,
        device_id: Uuid,
        sink_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.create_sink(sink_id, data).await,
            None => todo!(),
        }
    }

    pub async fn search_sinks(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSinksResp> {
        match self.devices.get(&device_id) {
            Some(device) => device.search_sinks(page, size).await,
            None => todo!(),
        }
    }

    pub async fn update_sink(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(mut device) => device.update_sink(sink_id, data).await,
            None => todo!(),
        }
    }

    pub async fn delete_sink(&self, device_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(_) => todo!(),
            None => todo!(),
        }
    }

    pub async fn create_sink_point(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        point_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        match self.devices.get_mut(&device_id) {
            Some(_) => todo!(),
            None => todo!(),
        }
    }

    pub async fn search_sink_points(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchPointResp> {
        match self.devices.get(&device_id) {
            Some(_) => todo!(),
            None => todo!(),
        }
    }

    pub async fn update_sink_point(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        point_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        match self.devices.get(&device_id) {
            Some(_) => todo!(),
            None => todo!(),
        }
    }

    pub async fn delete_sink_points(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        point_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        match self.devices.get(&device_id) {
            Some(_) => todo!(),
            None => todo!(),
        }
    }
}
