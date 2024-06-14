#![feature(lazy_cell)]

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use modbus::device::Modbus;
use serde::Serialize;
use std::{collections::HashMap, sync::LazyLock};
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error};
use types::device::{
    CreateDeviceReq, CreateGroupReq, CreatePointReq, DeviceDetailResp, ListDevicesResp,
    ListGroupsResp, ListPointResp, UpdateDeviceReq, UpdateGroupReq, WritePointValueReq,
};
use uuid::Uuid;

mod modbus;

pub static GLOBAL_DEVICE_MANAGER: LazyLock<DeviceManager> = LazyLock::new(|| DeviceManager {
    devices: RwLock::new(HashMap::new()),
});

pub struct DeviceManager {
    devices: RwLock<HashMap<Uuid, Box<dyn Device>>>,
}

impl DeviceManager {
    pub async fn create_device(
        &self,
        device_id: Option<Uuid>,
        req: CreateDeviceReq,
    ) -> HaliaResult<()> {
        let (device_id, create) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };

        let device = match req.r#type.as_str() {
            "modbus" => match Modbus::new(device_id, &req) {
                Ok(device) => device,
                Err(e) => {
                    debug!("create device err:{}", e);
                    return Err(e);
                }
            },
            _ => return Err(HaliaError::ProtocolNotSupported),
        };
        self.devices.write().await.insert(device_id, device);

        if create {
            persistence::device::insert(device_id, serde_json::to_string(&req)?).await?;
        }

        Ok(())
    }

    pub async fn read_device(&self, device_id: Uuid) -> HaliaResult<DeviceDetailResp> {
        match self.devices.read().await.get(&device_id) {
            Some(device) => Ok(device.get_detail()),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_device(&self, device_id: Uuid, req: UpdateDeviceReq) -> HaliaResult<()> {
        match self.devices.write().await.get_mut(&device_id) {
            Some(device) => {
                device.update(&req).await?;
                persistence::device::update_conf(device_id, serde_json::to_string(&req)?).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn start_device(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.write().await.get_mut(&device_id) {
            Some(device) => {
                match device.start().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("device start err:{}", e);
                        return Err(e);
                    }
                }
                match persistence::device::update_status(device_id, persistence::Status::Runing)
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("storage update device err:{}", e);
                        Err(e.into())
                    }
                }
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn stop_device(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.write().await.get_mut(&device_id) {
            Some(device) => {
                device.stop().await;
                persistence::device::update_status(device_id, persistence::Status::Stopped).await?;
                Ok(())
            }
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

    pub async fn delete_device(&self, device_id: Uuid) -> HaliaResult<()> {
        match self.devices.write().await.get_mut(&device_id) {
            Some(device) => device.stop().await,
            None => return Err(HaliaError::NotFound),
        };

        self.devices.write().await.remove(&device_id);
        persistence::device::delete(device_id).await?;

        Ok(())
    }

    pub async fn create_group(
        &self,
        device_id: Uuid,
        group_id: Option<Uuid>,
        req: CreateGroupReq,
    ) -> HaliaResult<()> {
        match self.devices.write().await.get_mut(&device_id) {
            Some(device) => {
                device.create_group(group_id, &req).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn read_groups(&self, device_id: Uuid) -> HaliaResult<Vec<ListGroupsResp>> {
        match self.devices.read().await.get(&device_id) {
            Some(device) => device.read_groups().await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_group(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        req: &UpdateGroupReq,
    ) -> HaliaResult<()> {
        match self.devices.read().await.get(&device_id) {
            Some(device) => device.update_group(group_id, req).await,
            None => {
                debug!("未找到设备");
                Err(HaliaError::NotFound)
            }
        }
    }

    pub async fn delete_groups(&self, device_id: Uuid, group_ids: Vec<Uuid>) -> HaliaResult<()> {
        match self.devices.write().await.get_mut(&device_id) {
            Some(device) => device.delete_groups(group_ids).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_points(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        create_points: Vec<(Option<Uuid>, CreatePointReq)>,
    ) -> HaliaResult<()> {
        match self.devices.read().await.get(&device_id) {
            Some(device) => {
                device.create_points(group_id, create_points).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn get_points(
        &self,
        device_id: Uuid,
        group_id: Uuid,
    ) -> HaliaResult<Vec<ListPointResp>> {
        match self.devices.read().await.get(&device_id) {
            Some(device) => device.read_points(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn read_points(
        &self,
        device_id: Uuid,
        group_id: Uuid,
    ) -> HaliaResult<Vec<ListPointResp>> {
        match self.devices.read().await.get(&device_id) {
            Some(device) => device.read_points(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Uuid,
        req: &CreatePointReq,
    ) -> HaliaResult<()> {
        match self.devices.read().await.get(&device_id) {
            Some(device) => device.update_point(group_id, point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn write_point_value(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Uuid,
        req: &WritePointValueReq,
    ) -> HaliaResult<()> {
        match self.devices.read().await.get(&device_id) {
            Some(device) => device.write_point_value(group_id, point_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_points(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        match self.devices.read().await.get(&device_id) {
            Some(device) => device.delete_points(group_id, point_ids).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn subscribe(
        &self,
        device_id: Uuid,
        group_id: Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.devices.read().await.get(&device_id) {
            Some(device) => device.subscribe(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }
}

impl DeviceManager {
    pub async fn recover(&self) -> HaliaResult<()> {
        let devices = persistence::device::read().await?;
        for (id, status, data) in devices {
            let req = match serde_json::from_str::<CreateDeviceReq>(data.as_str()) {
                Ok(req) => req,
                Err(e) => {
                    error!("{}", e);
                    return Err(e.into());
                }
            };
            match self.create_device(Some(id), req).await {
                Ok(_) => {}
                Err(_) => todo!(),
            }
            self.recover_group(id).await?;
            match status {
                persistence::Status::Stopped => {}
                persistence::Status::Runing => self.start_device(id).await?,
            }
        }
        Ok(())
    }

    async fn recover_group(&self, device_id: Uuid) -> HaliaResult<()> {
        let groups = persistence::group::read(device_id).await?;
        let groups: Vec<(Option<Uuid>, CreateGroupReq)> = groups
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

    async fn recover_points(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        let points = persistence::point::read(device_id, group_id).await?;
        let points: Vec<(Option<Uuid>, CreatePointReq)> = points
            .into_iter()
            .map(|(id, data)| {
                let req: CreatePointReq = serde_json::from_str(data.as_str()).unwrap();
                (Some(id), req)
            })
            .collect();
        self.create_points(device_id, group_id, points).await?;
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
    // device
    fn get_detail(&self) -> DeviceDetailResp;
    fn get_info(&self) -> ListDevicesResp;
    async fn start(&mut self) -> HaliaResult<()>;
    async fn stop(&mut self);
    async fn update(&mut self, req: &UpdateDeviceReq) -> HaliaResult<()>;

    // group
    async fn create_group(
        &mut self,
        group_id: Option<Uuid>,
        create_group: &CreateGroupReq,
    ) -> HaliaResult<()>;
    async fn read_groups(&self) -> HaliaResult<Vec<ListGroupsResp>>;
    async fn update_group(&self, group_id: Uuid, req: &UpdateGroupReq) -> HaliaResult<()>;
    async fn delete_groups(&self, ids: Vec<Uuid>) -> HaliaResult<()>;

    // points
    async fn create_points(
        &self,
        group_id: Uuid,
        create_points: Vec<(Option<Uuid>, CreatePointReq)>,
    ) -> HaliaResult<()>;
    async fn read_points(&self, group_id: Uuid) -> HaliaResult<Vec<ListPointResp>>;
    async fn update_point(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        req: &CreatePointReq,
    ) -> HaliaResult<()>;
    async fn write_point_value(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        req: &WritePointValueReq,
    ) -> HaliaResult<()>;
    async fn delete_points(&self, group_id: Uuid, point_ids: Vec<Uuid>) -> HaliaResult<()>;

    async fn subscribe(&self, group_id: Uuid) -> HaliaResult<broadcast::Receiver<MessageBatch>>;
}
