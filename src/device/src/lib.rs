use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use message::MessageBatch;
use modbus::Modbus;
use serde::Serialize;
use std::sync::LazyLock;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error};
use types::{
    apps::SearchSinkResp,
    device::{
        device::{
            CreateDeviceReq, SearchDeviceItemResp, SearchDeviceResp, SearchSinksResp,
            UpdateDeviceReq,
        },
        group::{CreateGroupReq, SearchGroupResp, UpdateGroupReq},
        point::{CreatePointReq, SearchPointResp, WritePointValueReq},
    },
    SearchResp,
};
use uuid::Uuid;

// mod coap;
mod modbus;
// mod opcua;

pub static GLOBAL_DEVICE_MANAGER: LazyLock<DeviceManager> = LazyLock::new(|| DeviceManager {
    devices: RwLock::new(vec![]),
    modbus_devices: RwLock::new(vec![]),
});

pub struct DeviceManager {
    devices: RwLock<Vec<(&'static str, Uuid)>>,
    modbus_devices: RwLock<Vec<Modbus>>,
}

impl DeviceManager {
    pub async fn search_devices(&self, page: usize, size: usize) -> SearchDeviceResp {
        let mut data = vec![];
        let mut i = 0;
        let mut total = 0;
        let mut err_cnt = 0;
        let mut close_cnt = 0;
        for (r#type, device_id) in self.devices.read().await.iter().rev() {
            match r#type {
                &modbus::TYPE => {
                    match self
                        .modbus_devices
                        .read()
                        .await
                        .iter()
                        .find(|device| device.id == *device_id)
                    {
                        Some(device) => {
                            let info = device.search();
                            if *&info.err {
                                err_cnt += 1;
                            }
                            if !*&info.on {
                                close_cnt += 1;
                            }
                            if i >= (page - 1) * size && i < page * size {
                                data.push(info);
                            }
                            total += 1;
                            i += 1;
                        }
                        None => panic!("无法获取modbus设备"),
                    }
                }
                _ => {}
            }
        }

        SearchDeviceResp {
            total,
            err_cnt,
            close_cnt,
            data,
        }
    }
}

// for modbus
impl DeviceManager {
    pub async fn modbus_create(&self, device_id: Option<Uuid>, data: String) -> HaliaResult<()> {
        match modbus::new(device_id, &data).await {
            Ok(device) => {
                self.devices.write().await.push((modbus::TYPE, device.id));
                self.modbus_devices.write().await.push(device);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn modbus_update(&self, device_id: Uuid, data: String) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.update(data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_start(&self, device_id: Uuid) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => Ok(device.start().await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_stop(&self, device_id: Uuid) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => Ok(device.stop().await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_delete(&self, device_id: Uuid) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.delete().await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_create_group(
        &self,
        device_id: Uuid,
        group_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.create_group(group_id, data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_search_groups(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupResp> {
        match self
            .modbus_devices
            .read()
            .await
            .iter()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.search_groups(page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_update_group(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.update_group(group_id, data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_delete_group(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.delete_group(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_create_group_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.create_group_point(group_id, point_id, data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_search_group_points(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchPointResp> {
        match self
            .modbus_devices
            .read()
            .await
            .iter()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.search_group_points(group_id, page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_update_group_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.update_group_point(group_id, point_id, data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_write_group_point_value(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.write_point_value(group_id, point_id, data).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_delete_group_points(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        match self
            .modbus_devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.id == device_id)
        {
            Some(device) => device.delete_group_points(group_id, point_ids).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn modbus_create_sink(
        &self,
        device_id: Uuid,
        sink_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_search_sinks(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSinksResp> {
        todo!()
    }

    pub async fn modbus_update_sink(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_delete_sink(&self, device_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_create_sink_point(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        point_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_search_sink_points(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchPointResp> {
        todo!()
    }

    pub async fn modbus_update_sink_point(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        point_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_delete_sink_points(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        point_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        todo!()
    }
}
