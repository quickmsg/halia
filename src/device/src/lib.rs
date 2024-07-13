use async_trait::async_trait;
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
        for (r#type, device_id) in self.devices.read().await.iter().skip((page - 1) * size) {
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
                            let info = device.get_info();
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
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };
        match modbus::new(device_id, &data) {
            Ok(device) => {
                self.devices.write().await.push((modbus::TYPE, device_id));
                self.modbus_devices.write().await.push(device);
                Ok(())
                // TODO 持久化
            }
            Err(e) => Err(e),
        }
    }

    pub async fn modbus_search(&self, device_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_update(&self, device_id: Uuid, data: String) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_start(&self, device_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_stop(&self, device_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_delete(&self, devie_id: Uuid) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_create_group(
        &self,
        device_id: Uuid,
        group_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_search_groups(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_update_group(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        todo!()
    }

    pub async fn modbus_delete_group(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        todo!()
    }
}

impl DeviceManager {
    async fn do_create_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Uuid,
        req: CreatePointReq,
    ) -> HaliaResult<()> {
        todo!()
        // match self
        //     .devices
        //     .read()
        //     .await
        //     .iter()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         // device.create_point(group_id, point_id, req).await?;
        //         // Ok(())
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
    }
}

impl DeviceManager {
    pub async fn create_device(&self, device_id: Option<Uuid>, data: String) -> HaliaResult<()> {
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };
        let req: CreateDeviceReq = serde_json::from_str(&data)?;
        // let resp = match req.r#type.as_str() {
        //     modbus::TYPE => {
        //         let device = modbus::new(device_id, req)?;
        //         if new {
        //             persistence::device::insert_modbus_device(&device_id, &data).await?;
        //         }
        //         Ok(device)
        //     }
        //     // opcua::TYPE => opcua::new(device_id, req),
        //     // coap::TYPE => {
        //     //     let device = coap::new(device_id, req)?;
        //     //     if new {
        //     //         persistence::device::insert_coap_device(&device_id, &data).await?;
        //     //     }
        //     //     Ok(device)
        //     // }
        //     _ => return Err(HaliaError::ProtocolNotSupported),
        // };

        // match resp {
        //     Ok(device) => self.devices.write().await.push(device),
        //     Err(e) => {
        //         debug!("create device err:{}", e);
        //         return Err(e);
        //     }
        // }
        Ok(())
    }

    pub async fn update_device(&self, device_id: Uuid, data: &Bytes) -> HaliaResult<()> {
        // let req: UpdateDeviceReq = serde_json::from_slice(data)?;
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         device.update(&req).await?;
        //     }
        //     None => return Err(HaliaError::NotFound),
        // }

        // unsafe { persistence::device::update_device_conf(device_id, data) }.await?;
        // Ok(())
        todo!()
    }

    pub async fn start_device(&self, device_id: Uuid) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         device.start().await;
        //         match persistence::device::update_device_status(
        //             device_id,
        //             persistence::Status::Runing,
        //         )
        //         .await
        //         {
        //             Ok(_) => Ok(()),
        //             Err(e) => {
        //                 error!("storage update device err:{}", e);
        //                 Err(e.into())
        //             }
        //         }
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn stop_device(&self, device_id: Uuid) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         device.stop().await;
        //         persistence::device::update_device_status(device_id, persistence::Status::Stopped)
        //             .await?;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    // pub async fn search_devices(&self, page: usize, size: usize) -> SearchDeviceResp {
    // let mut resp = vec![];
    // let mut i = 0;
    // let mut total = 0;
    // let mut err_cnt = 0;
    // let mut close_cnt = 0;
    // for device in self.devices.read().await.iter().rev() {
    //     let info = device.get_info().await;

    //     if *&info.err {
    //         err_cnt += 1;
    //     }
    //     if !*&info.on {
    //         close_cnt += 1;
    //     }
    //     if i >= (page - 1) * size && i < page * size {
    //         resp.push(info);
    //     }
    //     total += 1;
    //     i += 1;
    // }

    // SearchDeviceResp {
    //     total,
    //     err_cnt,
    //     close_cnt,
    //     data: resp,
    // }
    // todo!()
    // }

    pub async fn delete_device(&self, device_id: Uuid) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => device.stop().await,
        //     None => return Err(HaliaError::NotFound),
        // };

        // self.devices
        //     .write()
        //     .await
        //     .retain(|device| device.get_id() != device_id);
        // persistence::device::delete_device(&device_id).await?;

        Ok(())
    }

    pub async fn create_group(
        &self,
        device_id: Uuid,
        group_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         let (group_id, new) = match group_id {
        //             Some(group_id) => (group_id, false),
        //             None => (Uuid::new_v4(), true),
        //         };
        //         let create_group_req: CreateGroupReq = serde_json::from_str(&data)?;
        //         // device.create_group(group_id, &create_group_req).await?;
        //         if new {
        //             persistence::device::insert_group(&device_id, &group_id, &data).await?;
        //         }
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn search_groups(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupResp> {
        // match self
        //     .devices
        //     .read()
        //     .await
        //     .iter()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         // device.search_groups(page, size).await;
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn update_group(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        req: Bytes,
    ) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .read()
        //     .await
        //     .iter()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         let update_group_req: UpdateGroupReq = serde_json::from_slice(&req)?;
        //         // device.update_group(group_id, update_group_req).await?;
        //         persistence::device::update_group(&device_id, &group_id, req).await?;
        //         Ok(())
        //     }
        //     None => {
        //         debug!("未找到设备");
        //         Err(HaliaError::NotFound)
        //     }
        // }
        todo!()
    }

    pub async fn delete_group(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         // device.delete_group(group_id).await?;
        //         persistence::device::delete_group(&device_id, &group_id).await?;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn create_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        req: String,
    ) -> HaliaResult<()> {
        let point_id = Uuid::new_v4();
        let creaet_point_req = serde_json::from_str(&req)?;
        self.do_create_point(device_id, group_id, point_id, creaet_point_req)
            .await?;
        match persistence::device::insert_point(&device_id, &group_id, &point_id, &req).await {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("写入点位失败:{e:?}");
                self.delete_points(device_id, group_id, vec![point_id])
                    .await?;
                return Err(HaliaError::IoErr);
            }
        }
    }

    pub async fn search_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchPointResp> {
        // match self
        //     .devices
        //     .read()
        //     .await
        //     .iter()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         //  device.search_point(group_id, page, size).await,
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn update_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Uuid,
        req: &CreatePointReq,
    ) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .read()
        //     .await
        //     .iter()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         // device.update_point(group_id, point_id, req).await,
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn write_point_value(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Uuid,
        req: WritePointValueReq,
    ) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .read()
        //     .await
        //     .iter()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         // device
        //         //     .write_point_value(group_id, point_id, req.value)
        //         //     .await
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn delete_points(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_ids: Vec<Uuid>,
    ) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .read()
        //     .await
        //     .iter()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         // device.delete_points(&group_id, &point_ids).await?;
        //         let _ = persistence::device::delete_points(&device_id, &group_id, &point_ids).await;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn create_sink(
        &self,
        device_id: Uuid,
        sink_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        let (sink_id, new) = match sink_id {
            Some(sink_id) => (sink_id, true),
            None => (Uuid::new_v4(), false),
        };
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         // device.create_sink(sink_id, &data).await?,
        //     }
        //     None => return Err(HaliaError::NotFound),
        // }
        if new {
            if let Err(e) = persistence::device::insert_sink(&device_id, &sink_id, &data).await {
                debug!("create sink err :{e}");
                self.delete_sink(device_id, sink_id).await;
            }
        }

        Ok(())
    }

    pub async fn search_sinks(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSinksResp> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .rev()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         // Ok(device.search_sinks(page, size).await),
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn update_sink(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        data: String,
    ) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         // device.update_sink(sink_id, &data).await?;
        //         // persistence::device::update_sink(&device_id, &sink_id, &data).await?;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn delete_sink(&self, device_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         // device.delete_sink(sink_id).await?;
        //         persistence::device::delete_sink(&device_id, &sink_id).await?;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }
    pub async fn add_subscription(&self, device_id: Uuid, req: Bytes) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .read()
        //     .await
        //     .iter()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         //  device.add_subscription(req).await,
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn create_path(
        &self,
        device_id: Uuid,
        path_id: Option<Uuid>,
        data: String,
    ) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         let (path_id, new) = match path_id {
        //             Some(path_id) => (path_id, false),
        //             None => (Uuid::new_v4(), true),
        //         };
        //         // device.add_path(path_id, &data).await?;
        //         if new {
        //             persistence::device::insert_coap_path(&device_id, &path_id, &data).await?;
        //         }

        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn search_paths(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchResp> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         //  device.search_paths(page, size).await,
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn update_path(&self, device_id: Uuid, path_id: Uuid, req: Bytes) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         // device.update_path(path_id, req).await,
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }
}

// recover
impl DeviceManager {
    async fn recover_device(
        &self,
        device_id: Uuid,
        status: Status,
        data: String,
    ) -> HaliaResult<()> {
        let req: CreateDeviceReq = serde_json::from_str(&data)?;
        // let resp = match req.r#type.as_str() {
        //     // modbus::TYPE => modbus::new(device_id, req),
        //     // opcua::TYPE => opcua::new(device_id, req),
        //     // coap::TYPE => coap::new(device_id, req),
        //     _ => return Err(HaliaError::ProtocolNotSupported),
        // };

        // match resp {
        //     Ok(mut device) => {
        //         device.recover(status).await?;
        //         // self.devices.write().await.push(device);
        //     }
        //     Err(e) => {
        //         debug!("recover device err:{}", e);
        //         return Err(e);
        //     }
        // }

        Ok(())
    }

    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::device::read_devices().await {
            Ok(devices) => {
                for (id, status, data) in devices {
                    if let Err(e) = self.recover_device(id, status, data).await {
                        error!("{}", e);
                        return Err(e.into());
                    }
                }

                Ok(())
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => match persistence::device::init().await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("{e}");
                        return Err(e.into());
                    }
                },
                std::io::ErrorKind::PermissionDenied => todo!(),
                std::io::ErrorKind::ConnectionRefused => todo!(),
                std::io::ErrorKind::ConnectionReset => todo!(),
                std::io::ErrorKind::ConnectionAborted => todo!(),
                std::io::ErrorKind::NotConnected => todo!(),
                std::io::ErrorKind::AddrInUse => todo!(),
                std::io::ErrorKind::AddrNotAvailable => todo!(),
                std::io::ErrorKind::BrokenPipe => todo!(),
                std::io::ErrorKind::AlreadyExists => todo!(),
                std::io::ErrorKind::WouldBlock => todo!(),
                std::io::ErrorKind::InvalidInput => todo!(),
                std::io::ErrorKind::InvalidData => todo!(),
                std::io::ErrorKind::TimedOut => todo!(),
                std::io::ErrorKind::WriteZero => todo!(),
                std::io::ErrorKind::Interrupted => todo!(),
                std::io::ErrorKind::Unsupported => todo!(),
                std::io::ErrorKind::UnexpectedEof => todo!(),
                std::io::ErrorKind::OutOfMemory => todo!(),
                std::io::ErrorKind::Other => todo!(),
                _ => todo!(),
                // error!("read device from file err:{}", e);
                // return Err(e.into());
            },
        }
    }

    async fn recover_groups(&self, device_id: Uuid) -> HaliaResult<()> {
        let groups = match persistence::device::read_groups(&device_id).await {
            Ok(groups) => groups,
            Err(e) => {
                error!("read device:{} group from file err:{}", device_id, e);
                return Err(e.into());
            }
        };

        for (group_id, data) in groups {
            if let Err(e) = self.create_group(device_id, Some(group_id), data).await {
                error!(
                    "create device:{} group:{:?} err:{} ",
                    device_id, group_id, e
                );
                return Err(e);
            }
            if let Err(e) = self.recover_points(device_id, group_id).await {
                error!(
                    "recover device:{} group:{:?} err:{}",
                    device_id, group_id, e
                );
                return Err(e);
            }
        }

        Ok(())
    }

    async fn recover_sinks(&self, device_id: Uuid) -> HaliaResult<()> {
        let sinks = match persistence::device::read_sinks(&device_id).await {
            Ok(sinks) => sinks,
            Err(e) => {
                error!("read device:{} sinks from file err:{}", device_id, e);
                return Err(e.into());
            }
        };

        for (sink_id, data) in sinks {
            if let Err(e) = self.create_sink(device_id, Some(sink_id), data).await {
                error!("create device:{} sink:{:?} err:{} ", device_id, sink_id, e);
                return Err(e);
            }
        }

        Ok(())
    }

    async fn recover_points(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        let points = match persistence::device::read_points(&device_id, &group_id).await {
            Ok(points) => points,
            Err(e) => {
                error!(
                    "read device:{} group:{} points data err:{}",
                    device_id, group_id, e
                );
                return Err(e.into());
            }
        };

        for (point_id, point) in points {
            let req: CreatePointReq = serde_json::from_str(&point)?;
            if let Err(e) = self
                .do_create_point(device_id, group_id, point_id, req)
                .await
            {
                return Err(e);
            }
        }
        Ok(())
    }
}

// source and sink
impl DeviceManager {
    pub async fn subscribe(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == *device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         // device.subscribe(group_id).await,
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn unsubscribe(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         // device.unsubscribe(group_id).await,
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    pub async fn publish(
        &self,
        device_id: &Uuid,
        group_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        // match self
        //     .devices
        //     .write()
        //     .await
        //     .iter_mut()
        //     .find(|device| device.get_id() == *device_id)
        // {
        //     Some(device) => {
        //         todo!()
        //         // device.publish(group_id).await,
        //     }
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
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
    fn get_id(&self) -> Uuid;

    // 从持久化系统中恢复
    async fn recover(&mut self, status: Status) -> HaliaResult<()>;

    async fn start(&mut self);
    async fn stop(&mut self);
    async fn update(&mut self, req: &UpdateDeviceReq) -> HaliaResult<()>;

    // group
    // async fn create_group(
    //     &mut self,
    //     _group_id: Uuid,
    //     _create_group: &CreateGroupReq,
    // ) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn search_groups(&self, _page: usize, _size: usize) -> HaliaResult<SearchGroupResp> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn update_group(&self, _group_id: Uuid, _req: UpdateGroupReq) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn delete_group(&self, _group_id: Uuid) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }

    // async fn add_subscription(&self, _req: Bytes) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn update_subscription(
    //     &self,
    //     _subscription_id: Uuid,
    //     _req: serde_json::Value,
    // ) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn search_subscriptions(&self, _page: usize, _size: usize) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn delete_subscription(&self, _subscription_id: Uuid) -> HaliaResult<()> {
    //     Err(HaliaError::NotFound)
    // }

    // points
    // async fn create_point(
    //     &self,
    //     _group_id: Uuid,
    //     _point_id: Uuid,
    //     _req: CreatePointReq,
    // ) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }

    // async fn search_point(
    //     &self,
    //     _group_id: Uuid,
    //     _page: usize,
    //     _size: usize,
    // ) -> HaliaResult<SearchPointResp> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn update_point(
    //     &self,
    //     _group_id: Uuid,
    //     _point_id: Uuid,
    //     _req: &CreatePointReq,
    // ) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn write_point_value(
    //     &self,
    //     _group_id: Uuid,
    //     _point_id: Uuid,
    //     _value: serde_json::Value,
    // ) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn delete_points(&self, _group_id: &Uuid, _point_ids: &Vec<Uuid>) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }

    // // coap协议
    // async fn add_path(&mut self, _id: Uuid, _data: &String) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn search_paths(&self, _page: usize, _size: usize) -> HaliaResult<SearchResp> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn update_path(&self, _path_id: Uuid, _req: Bytes) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn delete_path(&self, _req: Bytes) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }

    // async fn subscribe(&mut self, id: &Uuid) -> HaliaResult<broadcast::Receiver<MessageBatch>>;
    // async fn unsubscribe(&mut self, id: Uuid) -> HaliaResult<()>;

    // async fn create_sink(&mut self, sink_id: Uuid, data: &String) -> HaliaResult<()>;
    // async fn search_sinks(&self, page: usize, size: usize) -> SearchSinksResp;
    // async fn update_sink(&mut self, sink_id: Uuid, data: &String) -> HaliaResult<()>;
    // async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()>;
    // async fn create_sink_item(
    //     &mut self,
    //     sink_id: Uuid,
    //     item_id: Uuid,
    //     data: &String,
    // ) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn search_sink_items(&self, _page: usize, _size: usize) -> HaliaResult<()> {
    //     Err(HaliaError::ProtocolNotSupported)
    // }
    // async fn update_sink_item(&mut self, sink_id: Uuid, item_id: &String) -> HaliaResult<()> {
    //     Err(HaliaError::DevicePointNotSupportWriteMethod)
    // }
    // async fn delete_sink_items(&mut self, sink_id: Uuid, item_ids: Vec<Uuid>) -> HaliaResult<()> {
    //     Err(HaliaError::DevicePointNotSupportWriteMethod)
    // }

    // async fn publish(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>>;
}
