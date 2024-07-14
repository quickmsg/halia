use bytes::Bytes;
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use dashmap::DashMap;
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
pub mod modbus;
// mod opcua;

pub static GLOBAL_DEVICE_MANAGER: LazyLock<DeviceManager> = LazyLock::new(|| DeviceManager {
    devices: RwLock::new(vec![]),
    modbus_devices: DashMap::new(),
});

pub struct DeviceManager {
    devices: RwLock<Vec<(&'static str, Uuid)>>,
    modbus_devices: DashMap<Uuid, Modbus>,
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
                &modbus::TYPE => match self.modbus_devices.get(device_id) {
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
}
