#![feature(lazy_cell)]

use anyhow::{bail, Result};
use async_trait::async_trait;
use modbus::device::Modbus;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    LazyLock,
};
use tokio::sync::RwLock;
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
    devices: RwLock<Vec<Box<dyn Device>>>,
}

impl DeviceManager {
    pub async fn create_device(&self, req: CreateDeviceReq) -> Result<()> {
        let id = self.auto_increment_id.fetch_add(1, Ordering::SeqCst);
        let device = match req.r#type.as_str() {
            "modbus" => Modbus::new(&req, id)?,
            _ => bail!("不支持协议"),
        };

        storage::insert_device(id, serde_json::to_string(&req)?).await?;
        self.devices.write().await.push(device);

        Ok(())
    }

    pub async fn read_device(&self, id: u64) -> Result<DeviceDetailResp> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == id)
        {
            Some(device) => Ok(device.get_detail()),
            None => bail!("未找到设备：{}。", id),
        }
    }

    pub async fn update_device(&self, id: u64, conf: Value) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == id)
        {
            Some(device) => device.update(conf).await,
            None => bail!("未找到设备：{}。", id),
        }
    }

    pub async fn start_device(&self, id: u64) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == id)
        {
            Some(device) => device.start().await,
            None => bail!("not find device"),
        }
    }

    pub async fn stop_device(&self, id: u64) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == id)
        {
            Some(device) => Ok(device.stop().await),
            None => bail!("not find device"),
        }
    }

    pub async fn read_devices(&self) -> Vec<ListDevicesResp> {
        self.devices
            .read()
            .await
            .iter()
            .map(|device| device.get_info())
            .collect()
    }

    pub async fn delete_device(&self, id: u64) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == id)
        {
            Some(device) => device.stop().await,
            None => bail!("未找到设备：{}。", id),
        };

        storage::delete_device(id).await?;
        self.devices
            .write()
            .await
            .retain(|device| device.get_id() != id);
        Ok(())
    }

    pub async fn create_group(&self, id: u64, create_group: CreateGroupReq) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| device.get_id() == id)
        {
            Some(device) => device.create_group(create_group).await,
            None => bail!("not find"),
        }
    }

    pub async fn read_groups(&self, id: u64) -> Result<Vec<ListGroupsResp>> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == id)
        {
            Some(device) => device.read_groups().await,
            None => bail!("not find"),
        }
    }

    pub async fn update_group(
        &self,
        device_id: u64,
        group_id: u64,
        update_group: Value,
    ) -> Result<()> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.update_group(group_id, update_group).await,
            None => bail!("未找到设备：{}。", device_id),
        }
    }

    pub async fn delete_groups(&self, id: u64, group_ids: Vec<u64>) -> Result<()> {
        match self
            .devices
            .write()
            .await
            .iter()
            .find(|device| device.get_id() == id)
        {
            Some(device) => device.delete_groups(group_ids).await,
            None => bail!("找不到该设备"),
        }
    }

    pub async fn create_points(
        &self,
        device_id: u64,
        group_id: u64,
        create_points: Vec<CreatePointReq>,
    ) -> Result<()> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.create_points(group_id, create_points).await,
            None => bail!("not find"),
        }
    }

    pub async fn get_points(&self, device_id: u64, group_id: u64) -> Result<Vec<ListPointResp>> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.read_points(group_id).await,
            None => bail!("未找到设备。"),
        }
    }

    pub async fn read_points(&self, device_id: u64, group_id: u64) -> Result<Vec<ListPointResp>> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.read_points(group_id).await,
            None => bail!("未找到设备：{}。", device_id),
        }
    }

    pub async fn update_point(
        &self,
        device_id: u64,
        group_id: u64,
        point_id: u64,
        update_point: Value,
    ) -> Result<()> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.update_point(group_id, point_id, update_point).await,
            None => bail!("未找到设备：{}。", device_id),
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
            .find(|device| device.get_id() == device_id)
        {
            Some(device) => device.delete_points(group_id, point_ids).await,
            None => bail!("未找到设备：{}。", device_id),
        }
    }
}

// impl DeviceManager {
//     pub async fn recover(&self) -> Result<()> {
//         let exists = self.file_exists(DEVICE_RECORD_FILE_NAME).await?;
//         if exists {
//             self.recover_device().await?;
//         }

//         Ok(())
//     }

//     async fn recover_device(&self) -> Result<()> {
//         match OpenOptions::new()
//             .read(true)
//             .open(DEVICE_RECORD_FILE_NAME)
//             .await
//         {
//             Ok(file) => {
//                 let mut buf_reader = tokio::io::BufReader::new(file);
//                 let mut buf = String::new();
//                 loop {
//                     match buf_reader.read_line(&mut buf).await {
//                         Ok(0) => return Ok(()),
//                         Ok(n) => {
//                             debug!("read buf :{:?}", buf);
//                             let record: DeviceRecord = match serde_json::from_str(&buf) {
//                                 Ok(v) => v,
//                                 Err(e) => bail!("{}", e),
//                             };

//                             let device = match record.req.r#type.as_str() {
//                                 "modbus" => match Modbus::new(&record.req, record.id) {
//                                     Ok(device) => device,
//                                     Err(e) => bail!("{}", e),
//                                 },
//                                 _ => bail!("not support"),
//                             };

//                             self.recover_groups(&device, record.id).await?;

//                             self.devices.write().await.push(device);
//                             buf.clear();
//                         }
//                         Err(e) => bail!("{}", e),
//                     }
//                 }
//             }
//             Err(e) => Err(e.into()),
//         }
//     }

//     async fn recover_groups(&self, device: &Box<dyn Device>, id: u64) -> Result<()> {
//         let group_dir_path = Path::new(DEVICE_RECORD_FILE_NAME).join(id.to_string());
//         match fs::read_dir(group_dir_path).await {
//             Ok(mut dir) => {
//                 while let Some(entry) = dir.next_entry().await? {
//                     match OpenOptions::new().read(true).open(entry.path()).await {
//                         Ok(file) => {
//                             let mut buf_reader = tokio::io::BufReader::new(file);
//                             let mut buf = String::new();
//                             loop {
//                                 match buf_reader.read_line(&mut buf).await {
//                                     Ok(0) => return Ok(()),
//                                     Ok(n) => {
//                                         debug!("read buf :{:?}", buf);
//                                         let record: GroupRecord = match serde_json::from_str(&buf) {
//                                             Ok(v) => v,
//                                             Err(e) => bail!("{}", e),
//                                         };

//                                         device.recover_group(record).await?;

//                                         buf.clear();
//                                     }
//                                     Err(e) => bail!("{}", e),
//                                 }
//                             }
//                         }
//                         Err(_) => todo!(),
//                     }
//                 }
//             }
//             Err(e) => bail!("{}", e),
//         }

//         Ok(())
//     }

//     async fn recover_points(&self) {}

//     async fn file_exists(&self, path: impl AsRef<Path>) -> Result<bool> {
//         let exists = fs::try_exists(path).await?;
//         Ok(exists)
//     }
// }

#[derive(Serialize)]
pub struct DeviceInfo {
    pub name: String,
    pub status: bool,
    pub r#type: String,
}

#[async_trait]
trait Device: Sync + Send {
    fn get_id(&self) -> u64;
    fn get_detail(&self) -> DeviceDetailResp;
    fn get_info(&self) -> ListDevicesResp;
    async fn start(&mut self) -> Result<()>;
    async fn stop(&mut self);
    async fn update(&mut self, conf: Value) -> Result<()>;

    // group
    async fn create_group(&mut self, create_group: CreateGroupReq) -> Result<()>;
    // async fn recover_group(&self, record: GroupRecord) -> Result<()>;
    async fn read_groups(&self) -> Result<Vec<ListGroupsResp>>;
    async fn update_group(&self, group_id: u64, update_group: Value) -> Result<()>;
    async fn delete_groups(&self, ids: Vec<u64>) -> Result<()>;

    // points
    async fn create_points(&self, group_id: u64, create_points: Vec<CreatePointReq>) -> Result<()>;
    async fn read_points(&self, group_id: u64) -> Result<Vec<ListPointResp>>;
    async fn update_point(&self, group_id: u64, point_id: u64, update_point: Value) -> Result<()>;
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
