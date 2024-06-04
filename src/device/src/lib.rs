#![feature(lazy_cell)]

use async_trait::async_trait;
use common::error::{HaliaError, Result};
use modbus::device::Modbus;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    LazyLock,
};
use tokio::sync::RwLock;
use tracing::debug;
use types::device::{
    CreateDeviceReq, CreateGroupReq, CreatePointReq, DeviceDetailResp, ListDevicesResp,
    ListGroupsResp, ListPointResp,
};

use serde::Serialize;

mod modbus;
pub mod storage;

pub static GLOBAL_DEVICE_MANAGER: LazyLock<DeviceManager> = LazyLock::new(|| DeviceManager {
    auto_increment_id: AtomicU64::new(1),
    devices: RwLock::new(vec![]),
});

pub struct DeviceManager {
    auto_increment_id: AtomicU64,
    devices: RwLock<Vec<(u64, Box<dyn Device>)>>,
}

impl DeviceManager {
    pub async fn create_device(&self, device_id: Option<u64>, req: CreateDeviceReq) -> Result<()> {
        let (device_id, backup) = match device_id {
            Some(device_id) => {
                if device_id > self.auto_increment_id.load(Ordering::SeqCst) {
                    self.auto_increment_id.store(device_id, Ordering::SeqCst);
                }
                (device_id, false)
            }
            None => (self.auto_increment_id.fetch_add(1, Ordering::SeqCst), true),
        };

        let device = match req.r#type.as_str() {
            "modbus" => Modbus::new(&req, device_id)?,
            _ => return Err(HaliaError::ProtocolNotSupported),
        };
        self.devices.write().await.push((device_id, device));
        if backup {
            storage::insert_device(device_id, serde_json::to_string(&req)?).await?;
        }

        Ok(())
    }

    pub async fn read_device(&self, device_id: u64) -> Result<DeviceDetailResp> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => Ok(device.get_detail()),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_device(&self, device_id: u64, conf: Value) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => {
                device.update(conf.clone()).await?;
                storage::update_device(device_id, serde_json::from_value(conf)?).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn start_device(&self, device_id: u64) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => device.start().await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn stop_device(&self, device_id: u64) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => Ok(device.stop().await),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn read_devices(&self) -> Vec<ListDevicesResp> {
        self.devices
            .read()
            .await
            .iter()
            .map(|(_, device)| device.get_info())
            .collect()
    }

    pub async fn delete_device(&self, device_id: u64) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => device.stop().await,
            None => return Err(HaliaError::NotFound),
        };

        storage::delete_device(device_id).await?;
        self.devices
            .write()
            .await
            .retain(|(id, _)| *id != device_id);
        Ok(())
    }

    pub async fn create_group(
        &self,
        device_id: u64,
        group_id: Option<u64>,
        req: CreateGroupReq,
    ) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => {
                device.create_group(group_id, &req).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn read_groups(&self, device_id: u64) -> Result<Vec<ListGroupsResp>> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => device.read_groups().await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_group(
        &self,
        device_id: u64,
        group_id: u64,
        req: &CreateGroupReq,
    ) -> Result<()> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => device.update_group(group_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_groups(&self, device_id: u64, group_ids: Vec<u64>) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => device.delete_groups(group_ids).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_points(
        &self,
        device_id: u64,
        group_id: u64,
        create_points: Vec<(Option<u64>, CreatePointReq)>,
    ) -> Result<()> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => {
                device.create_points(group_id, create_points).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn get_points(&self, device_id: u64, group_id: u64) -> Result<Vec<ListPointResp>> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => device.read_points(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn read_points(&self, device_id: u64, group_id: u64) -> Result<Vec<ListPointResp>> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => device.read_points(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_point(
        &self,
        device_id: u64,
        group_id: u64,
        point_id: u64,
        req: &CreatePointReq,
    ) -> Result<()> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => device.update_point(group_id, point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_points(
        &self,
        device_id: u64,
        group_id: u64,
        point_ids: Vec<u64>,
    ) -> Result<()> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|(id, _)| *id == device_id)
        {
            Some((_, device)) => device.delete_points(group_id, point_ids).await,
            None => Err(HaliaError::NotFound),
        }
    }
}

impl DeviceManager {
    pub async fn recover(&self) -> Result<()> {
        let devices = storage::read_devices().await?;
        debug!("read devices is :{:?}", devices);
        for (id, data) in devices {
            let req: CreateDeviceReq = serde_json::from_str(data.as_str())?;
            self.create_device(Some(id), req).await?;
            self.recover_group(id).await?;
        }
        Ok(())
    }

    async fn recover_group(&self, device_id: u64) -> Result<()> {
        let groups = storage::read_groups(device_id).await?;
        debug!("read groups is :{:?}", groups);
        let groups: Vec<(Option<u64>, CreateGroupReq)> = groups
            .into_iter()
            .map(|(id, data)| {
                let req: CreateGroupReq = serde_json::from_str(data.as_str()).unwrap();
                (Some(id), req)
            })
            .collect();

        for (group_id, req) in groups {
            self.create_group(device_id, group_id, req).await?;
            self.recover_points(device_id, group_id.unwrap()).await?;
        }

        Ok(())
    }

    async fn recover_points(&self, device_id: u64, group_id: u64) -> Result<()> {
        let points = storage::read_points(device_id, group_id).await?;
        let points: Vec<(Option<u64>, Value)> = points
            .into_iter()
            .map(|(id, data)| {
                let value: Value = serde_json::from_str(data.as_str()).unwrap();
                (Some(id), value)
            })
            .collect();
        self.create_points(device_id, group_id, points).await?;
        debug!("recover points done");
        Ok(())
    }
}

#[derive(Serialize)]
pub struct DeviceInfo {
    pub name: String,
    pub status: bool,
    pub r#type: String,
}

#[async_trait]
trait Device: Sync + Send {
    fn get_detail(&self) -> DeviceDetailResp;
    fn get_info(&self) -> ListDevicesResp;
    async fn start(&mut self) -> Result<()>;
    async fn stop(&mut self);
    async fn update(&mut self, conf: Value) -> Result<()>;

    // group
    async fn create_group(
        &mut self,
        group_id: Option<u64>,
        create_group: &CreateGroupReq,
    ) -> Result<()>;
    async fn read_groups(&self) -> Result<Vec<ListGroupsResp>>;
    async fn update_group(&self, group_id: u64, req: &CreateGroupReq) -> Result<()>;
    async fn delete_groups(&self, ids: Vec<u64>) -> Result<()>;

    // points
    async fn create_points(
        &self,
        group_id: u64,
        create_points: Vec<(Option<u64>, CreatePointReq)>,
    ) -> Result<()>;
    async fn read_points(&self, group_id: u64) -> Result<Vec<ListPointResp>>;
    async fn update_point(&self, group_id: u64, point_id: u64, req: &CreatePointReq) -> Result<()>;
    async fn delete_points(&self, group_id: u64, point_ids: Vec<u64>) -> Result<()>;
}

pub(crate) enum DataValue {
    Int16(i16),
    Uint16(u16),
    Int32(i32),
    Uint32(u32),
    Int64(i64),
    Uint64(u64),
    Float32(f32),
    Float64(f64),
    Bit,
    // TODO
    String,
    Bytes,
}
