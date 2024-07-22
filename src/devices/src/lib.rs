// use coap::manager::GLOBAL_COAP_MANAGER;
use common::{error::HaliaResult, persistence};
use modbus::manager::GLOBAL_MODBUS_MANAGER;
use opcua::manager::GLOBAL_OPCUA_MANAGER;
use std::{str::FromStr, sync::LazyLock};
use tokio::sync::RwLock;
use types::devices::{coap::CreateUpdateCoapReq, modbus::CreateUpdateModbusReq, SearchDevicesResp};

use uuid::Uuid;

// pub mod coap;
pub mod modbus;
pub mod opcua;

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

    pub async fn search(&self, page: usize, size: usize) -> SearchDevicesResp {
        let mut data = vec![];
        let mut i = 0;
        let mut total = 0;
        let mut err_cnt = 0;
        let mut close_cnt = 0;
        for (r#type, device_id) in self.devices.read().await.iter().rev() {
            let resp = match r#type {
                &modbus::TYPE => GLOBAL_MODBUS_MANAGER.search(device_id),
                &opcua::TYPE => GLOBAL_OPCUA_MANAGER.search(device_id),
                // &coap::TYPE => GLOBAL_COAP_MANAGER.search(device_id),
                _ => unreachable!(),
            };

            match resp {
                Ok(resp) => {
                    if *&resp.err {
                        err_cnt += 1;
                    }
                    if !*&resp.on {
                        close_cnt += 1;
                    }
                    if i >= (page - 1) * size && i < page * size {
                        data.push(resp);
                    }
                    total += 1;
                    i += 1;
                }
                Err(e) => panic!("无法获取"),
            }
        }

        SearchDevicesResp {
            total,
            err_cnt,
            close_cnt,
            data,
        }
    }

    pub async fn delete(&self, device_id: &Uuid) {
        self.devices.write().await.retain(|(_, id)| id == device_id);
    }

    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::devices::read_devices().await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    if items.len() != 4 {
                        panic!("数据损坏")
                    }

                    let device_id = Uuid::from_str(items[0]).unwrap();

                    match items[1] {
                        modbus::TYPE => {
                            let req: CreateUpdateModbusReq = serde_json::from_str(items[3])?;
                            GLOBAL_MODBUS_MANAGER.create(Some(device_id), req).await?;
                            GLOBAL_MODBUS_MANAGER.recover(&device_id).await.unwrap();
                            match items[2] {
                                "0" => {}
                                "1" => {
                                    GLOBAL_MODBUS_MANAGER.start(device_id).await.unwrap();
                                }
                                _ => panic!("文件已损坏"),
                            }
                        }
                        // coap::TYPE => {
                        //     let req: CreateUpdateCoapReq = serde_json::from_str(items[3])?;
                        //     GLOBAL_COAP_MANAGER.create(Some(device_id), req).await?;
                        //     GLOBAL_COAP_MANAGER.recover(&device_id).await.unwrap();
                        //     match items[2] {
                        //         "0" => {}
                        //         "1" => GLOBAL_COAP_MANAGER.start(device_id).await.unwrap(),
                        //         _ => panic!("文件已损坏"),
                        //     }
                        // }
                        _ => {}
                    }
                }
                Ok(())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => match persistence::devices::init().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.into()),
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
            },
        }
    }
}
