use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use serde::Serialize;
use std::sync::LazyLock;
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error};
use types::device::{
    device::{
        CreateDeviceReq, SearchDeviceItemResp, SearchDeviceResp, SearchSinksResp, UpdateDeviceReq,
    },
    group::{CreateGroupReq, SearchGroupResp, UpdateGroupReq},
    point::{CreatePointReq, SearchPointResp, WritePointValueReq},
};
use uuid::Uuid;

mod modbus;
mod opcua;

pub static GLOBAL_DEVICE_MANAGER: LazyLock<DeviceManager> = LazyLock::new(|| DeviceManager {
    devices: RwLock::new(vec![]),
});

pub struct DeviceManager {
    devices: RwLock<Vec<Box<dyn Device>>>,
}

impl DeviceManager {
    async fn do_create_device(&self, device_id: Uuid, req: &CreateDeviceReq) -> HaliaResult<()> {
        let device = match req.r#type.as_str() {
            modbus::TYPE => match modbus::new(device_id, req) {
                Ok(device) => device,
                Err(e) => {
                    debug!("create device err:{}", e);
                    return Err(e);
                }
            },
            _ => return Err(HaliaError::ProtocolNotSupported),
        };
        self.devices.write().await.push(device);
        Ok(())
    }

    async fn do_create_group(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        req: CreateGroupReq,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => {
                device.create_group(group_id, &req).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    async fn do_create_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        point_id: Uuid,
        req: CreatePointReq,
    ) -> HaliaResult<()> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => {
                device.create_point(group_id, point_id, req).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    async fn do_create_sink(&self, device_id: Uuid, sink_id: Uuid, req: Bytes) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.create_sink(sink_id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }
}

impl DeviceManager {
    pub async fn create_device(&self, body: &Bytes) -> HaliaResult<()> {
        let req: CreateDeviceReq = serde_json::from_slice(body)?;
        let device_id = Uuid::new_v4();
        self.do_create_device(device_id.clone(), &req).await?;
        persistence::device::insert_device(&device_id, body).await?;
        Ok(())
    }

    pub async fn update_device(&self, device_id: Uuid, data: &Bytes) -> HaliaResult<()> {
        let req: UpdateDeviceReq = serde_json::from_slice(data)?;
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => {
                device.update(&req).await?;
            }
            None => return Err(HaliaError::NotFound),
        }

        // data json解析成功的情况下，必定可以转换为string，这是安全的
        unsafe { persistence::device::update_device_conf(device_id, data) }.await?;
        Ok(())
    }

    pub async fn start_device(&self, device_id: Uuid) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => {
                match device.start().await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("device start err:{}", e);
                        return Err(e);
                    }
                }
                match persistence::device::update_device_status(
                    device_id,
                    persistence::Status::Runing,
                )
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
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => {
                device.stop().await;
                persistence::device::update_device_status(device_id, persistence::Status::Stopped)
                    .await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn search_devices(&self, page: usize, size: usize) -> SearchDeviceResp {
        let mut resp = vec![];
        let mut i = 0;
        let mut total = 0;
        let mut err_cnt = 0;
        let mut close_cnt = 0;
        for device in self.devices.read().await.iter().rev() {
            let info = device.get_info().await;

            if *&info.err {
                err_cnt += 1;
            }
            if !*&info.on {
                close_cnt += 1;
            }
            if i >= (page - 1) * size && i < page * size {
                resp.push(info);
            }
            total += 1;
            i += 1;
        }

        SearchDeviceResp {
            total,
            err_cnt,
            close_cnt,
            data: resp,
        }
    }

    pub async fn delete_device(&self, device_id: Uuid) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.stop().await,
            None => return Err(HaliaError::NotFound),
        };

        self.devices
            .write()
            .await
            .retain(|device| device.get_id() != device_id);
        persistence::device::delete_device(&device_id).await?;

        Ok(())
    }

    pub async fn create_group(&self, device_id: Uuid, req: Bytes) -> HaliaResult<()> {
        let group_id = Uuid::new_v4();
        let create_group_req: CreateGroupReq = serde_json::from_slice(&req)?;
        self.do_create_group(device_id, group_id, create_group_req)
            .await?;
        // TODO 插入失败删除group
        persistence::device::insert_group(&device_id, &group_id, &req).await?;
        Ok(())
    }

    pub async fn search_groups(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchGroupResp> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.search_groups(page, size).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn update_group(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        req: Bytes,
    ) -> HaliaResult<()> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => {
                let update_group_req: UpdateGroupReq = serde_json::from_slice(&req)?;
                device.update_group(group_id, update_group_req).await?;
                persistence::device::update_group(&device_id, &group_id, req).await?;
                Ok(())
            }
            None => {
                debug!("未找到设备");
                Err(HaliaError::NotFound)
            }
        }
    }

    pub async fn delete_group(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => {
                device.delete_group(group_id).await?;
                persistence::device::delete_group(&device_id, &group_id).await?;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_point(
        &self,
        device_id: Uuid,
        group_id: Uuid,
        req: Bytes,
    ) -> HaliaResult<()> {
        let point_id = Uuid::new_v4();
        let creaet_point_req = serde_json::from_slice(&req)?;
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
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.search_point(group_id, page, size).await,
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
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
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
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
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
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => {
                device.delete_points(&group_id, &point_ids).await?;
                let _ = persistence::device::delete_points(&device_id, &group_id, &point_ids).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn create_sink(&self, device_id: Uuid, req: Bytes) -> HaliaResult<()> {
        let sink_id = Uuid::new_v4();
        self.do_create_sink(device_id, sink_id, req.clone()).await?;
        match persistence::device::insert_sink(&device_id, &sink_id, &req).await {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("sink写入文件失败:{e:?}");
                // TODO delete
                Err(e.into())
            }
        }
    }

    pub async fn search_sinks(
        &self,
        device_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchSinksResp> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => Ok(device.search_sinks(page, size).await),
            None => Err(HaliaError::NotFound),
        }
    }
}

// recover
impl DeviceManager {
    async fn recover_device(&self, device_id: Uuid, data: String) -> HaliaResult<()> {
        let req: CreateDeviceReq = serde_json::from_str(&data)?;
        self.do_create_device(device_id, &req).await
    }

    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::device::read_devices().await {
            Ok(devices) => {
                for (id, status, data) in devices {
                    if let Err(e) = self.recover_device(id, data).await {
                        error!("{}", e);
                        return Err(e.into());
                    }
                    if let Err(e) = self.recover_group(id).await {
                        error!("recover group err:{}", e);
                        return Err(e.into());
                    }
                    if status == persistence::Status::Runing {
                        if let Err(e) = self.start_device(id).await {
                            error!("start device err:{}", e);
                            return Err(e.into());
                        }
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

    async fn recover_group(&self, device_id: Uuid) -> HaliaResult<()> {
        let groups = match persistence::device::read_groups(&device_id).await {
            Ok(groups) => groups,
            Err(e) => {
                error!("read device:{} group from file err:{}", device_id, e);
                return Err(e.into());
            }
        };

        for (group_id, req) in groups {
            let req: CreateGroupReq = serde_json::from_str(&req)?;
            if let Err(e) = self.do_create_group(device_id, group_id, req).await {
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
        device_id: Uuid,
        group_id: Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.subscribe(group_id).await,
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn unsubscribe(&self, device_id: Uuid, group_id: Uuid) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.unsubscribe(group_id).await,
            None => Err(HaliaError::NotFound),
        }
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
    async fn get_info(&self) -> SearchDeviceItemResp;
    async fn start(&mut self) -> HaliaResult<()>;
    async fn stop(&mut self);
    async fn update(&mut self, req: &UpdateDeviceReq) -> HaliaResult<()>;

    // group
    async fn create_group(
        &mut self,
        group_id: Uuid,
        create_group: &CreateGroupReq,
    ) -> HaliaResult<()>;

    async fn search_groups(&self, page: usize, size: usize) -> HaliaResult<SearchGroupResp>;
    async fn update_group(&self, group_id: Uuid, req: UpdateGroupReq) -> HaliaResult<()>;
    async fn delete_group(&self, group_id: Uuid) -> HaliaResult<()>;

    // points
    async fn create_point(
        &self,
        group_id: Uuid,
        point_id: Uuid,
        req: CreatePointReq,
    ) -> HaliaResult<()>;

    async fn search_point(
        &self,
        group_id: Uuid,
        page: usize,
        size: usize,
    ) -> HaliaResult<SearchPointResp>;
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
    async fn delete_points(&self, group_id: &Uuid, point_ids: &Vec<Uuid>) -> HaliaResult<()>;

    async fn subscribe(&mut self, group_id: Uuid)
        -> HaliaResult<broadcast::Receiver<MessageBatch>>;
    async fn unsubscribe(&mut self, group_id: Uuid) -> HaliaResult<()>;

    async fn create_sink(&self, sink_id: Uuid, req: Bytes) -> HaliaResult<()>;
    async fn search_sinks(&self, page: usize, size: usize) -> SearchSinksResp;
    async fn update_sink(&self, sink_id: Uuid, req: Bytes) -> HaliaResult<()>;
    async fn delete_sink(&self, sink_id: Uuid) -> HaliaResult<()>;
    async fn publish(&self, sink_id: Uuid) -> Result<mpsc::Sender<MessageBatch>>;
}
