use common::{error::HaliaResult, persistence};
use modbus::manager::GLOBAL_MODBUS_MANAGER;
use std::sync::LazyLock;
use tokio::sync::RwLock;
use types::device::device::SearchDeviceResp;

use uuid::Uuid;

// mod coap;
pub mod modbus;
// mod opcua;

pub static GLOBAL_DEVICE_MANAGER: LazyLock<DeviceManager> = LazyLock::new(|| DeviceManager {
    devices: RwLock::new(vec![]),
});

pub struct DeviceManager {
    devices: RwLock<Vec<(&'static str, Uuid)>>,
}

impl DeviceManager {
    pub async fn create(&self, r#type: &'static str, device_id: Uuid) {
        self.devices.write().await.push((r#type, device_id));
    }

    pub async fn search_devices(&self, page: usize, size: usize) -> SearchDeviceResp {
        let mut data = vec![];
        let mut i = 0;
        let mut total = 0;
        let mut err_cnt = 0;
        let mut close_cnt = 0;
        for (r#type, device_id) in self.devices.read().await.iter().rev() {
            match r#type {
                &modbus::TYPE => match GLOBAL_MODBUS_MANAGER.search(device_id) {
                    Ok(info) => {
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
                    Err(e) => panic!("无法获取modbus设备"),
                },
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

    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::device::read_devices().await {
            Ok(devices) => {
                for (device_id, mut datas) in devices {
                    if datas.len() != 3 {
                        panic!("数据损坏");
                    }
                    match datas[0].as_str() {
                        modbus::TYPE => {
                            GLOBAL_MODBUS_MANAGER
                                .create(Some(device_id), datas.pop().unwrap())
                                .await?;
                            GLOBAL_MODBUS_MANAGER.recover(&device_id).await.unwrap();
                            match datas[1].as_str() {
                                "0" => {}
                                "1" => {
                                    GLOBAL_MODBUS_MANAGER.start(device_id).await.unwrap();
                                }
                                _ => panic!("文件已损坏"),
                            }
                        }
                        _ => {}
                    }
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}
