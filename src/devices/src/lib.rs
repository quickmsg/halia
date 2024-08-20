#![feature(io_error_more)]
use std::{str::FromStr, sync::LazyLock};

use bytes::Bytes;
use coap::manager::GLOBAL_COAP_MANAGER;
use common::{error::HaliaResult, persistence};
use modbus::manager::GLOBAL_MODBUS_MANAGER;
use opcua::manager::GLOBAL_OPCUA_MANAGER;
use tokio::sync::RwLock;
use tracing::warn;
use types::{
    devices::{
        coap::CreateUpdateCoapReq, modbus::CreateUpdateModbusReq, opcua::CreateUpdateOpcuaReq, DeviceType, QueryParams, SearchDevicesItemResp, SearchDevicesResp, Summary
    },
    Pagination, SearchSourcesOrSinksResp,
};

use uuid::Uuid;

pub mod coap;
pub mod modbus;
pub mod opcua;

pub static GLOBAL_DEVICE_MANAGER: LazyLock<DeviceManager> = LazyLock::new(|| DeviceManager {
    devices: RwLock::new(vec![]),
});

pub struct DeviceManager {
    devices: RwLock<Vec<(DeviceType, Uuid)>>,
}

impl DeviceManager {
    pub async fn create(&self, typ: DeviceType, device_id: Uuid) {
        self.devices.write().await.push((typ, device_id));
    }

    pub fn check_duplicate_name(&self, device_id: &Option<Uuid>, name: &str) -> HaliaResult<()> {
        GLOBAL_MODBUS_MANAGER.check_duplicate_name(device_id, name)?;
        GLOBAL_COAP_MANAGER.check_duplicate_name(device_id, name)?;
        GLOBAL_OPCUA_MANAGER.check_duplicate_name(device_id, name)?;
        Ok(())
    }

    pub async fn get_summary(&self) -> Summary {
        let mut total = 0;
        let mut running_cnt = 0;
        let mut err_cnt = 0;
        let mut off_cnt = 0;
        for (typ, device_id) in self.devices.read().await.iter().rev() {
            let resp = match typ {
                DeviceType::Modbus => GLOBAL_MODBUS_MANAGER.search(device_id).await,
                DeviceType::Opcua => GLOBAL_OPCUA_MANAGER.search(device_id),
                DeviceType::Coap => GLOBAL_COAP_MANAGER.search(device_id),
            };

            match resp {
                Ok(resp) => {
                    total += 1;
                    if resp.err.is_some() {
                        err_cnt += 1;
                    } else {
                        if resp.on {
                            running_cnt += 1;
                        } else {
                            off_cnt += 1;
                        }
                    }
                }
                Err(e) => {
                    warn!("{}", e);
                }
            }
        }
        Summary {
            total,
            running_cnt,
            err_cnt,
            off_cnt,
        }
    }

    pub async fn search(
        &self,
        pagination: Pagination,
        query_params: QueryParams,
    ) -> SearchDevicesResp {
        let mut data = vec![];
        let mut i = 0;
        let mut total = 0;

        for (typ, device_id) in self.devices.read().await.iter().rev() {
            if let Some(query_type) = &query_params.typ {
                if typ != query_type {
                    continue;
                }
            }

            let resp = match typ {
                DeviceType::Modbus => GLOBAL_MODBUS_MANAGER.search(device_id).await,
                DeviceType::Opcua => GLOBAL_OPCUA_MANAGER.search(device_id),
                DeviceType::Coap => GLOBAL_COAP_MANAGER.search(device_id),
            };

            match resp {
                Ok(resp) => {
                    if let Some(query_name) = &query_params.name {
                        if !resp.conf.base.name.contains(query_name) {
                            continue;
                        }
                    }

                    if let Some(on) = &query_params.on {
                        if resp.on != *on {
                            continue;
                        }
                    }

                    if let Some(err) = &query_params.err {
                        if resp.err.is_some() != *err {
                            continue;
                        }
                    }

                    if i >= (pagination.page - 1) * pagination.size
                        && i < pagination.page * pagination.size
                    {
                        data.push(resp);
                    }
                    total += 1;
                    i += 1;
                }
                Err(e) => {
                    warn!("{}", e);
                }
            }
        }

        SearchDevicesResp { total, data }
    }

    pub async fn delete(&self, device_id: &Uuid) {
        self.devices.write().await.retain(|(_, id)| id != device_id);
    }

    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::devices::read_devices().await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 4);

                    let device_id = Uuid::from_str(items[0]).unwrap();

                    let typ = DeviceType::try_from(items[1]);
                    match typ {
                        Ok(typ) => match typ {
                            DeviceType::Modbus => {
                                let req: CreateUpdateModbusReq = serde_json::from_str(items[3])?;
                                GLOBAL_MODBUS_MANAGER.create(Some(device_id), req).await?;
                                GLOBAL_MODBUS_MANAGER.recover(&device_id).await.unwrap();
                                match items[2] {
                                    "0" => {}
                                    "1" => GLOBAL_MODBUS_MANAGER.start(device_id).await.unwrap(),
                                    _ => panic!("文件已损坏"),
                                }
                            }
                            DeviceType::Opcua => {
                                let req: CreateUpdateOpcuaReq = serde_json::from_str(items[3])?;
                                GLOBAL_OPCUA_MANAGER.create(Some(device_id), req).await?;
                                GLOBAL_OPCUA_MANAGER.recover(&device_id).await.unwrap();
                                match items[2] {
                                    "0" => {}
                                    "1" => GLOBAL_OPCUA_MANAGER.start(device_id).await.unwrap(),
                                    _ => panic!("文件已损坏"),
                                }
                            }
                            DeviceType::Coap => {
                                let req: CreateUpdateCoapReq = serde_json::from_str(items[3])?;
                                GLOBAL_COAP_MANAGER.create(Some(device_id), req).await?;
                                GLOBAL_COAP_MANAGER.recover(&device_id).await.unwrap();
                                match items[2] {
                                    "0" => {}
                                    "1" => GLOBAL_COAP_MANAGER.start(device_id).await.unwrap(),
                                    _ => panic!("文件已损坏"),
                                }
                            }
                        },
                        Err(e) => panic!("{}", e),
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