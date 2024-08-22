#![feature(io_error_more)]
use std::{str::FromStr, sync::LazyLock};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc, RwLock};
use types::{
    devices::{
        CreateUpdateDeviceReq, DeviceConf, DeviceType, QueryParams, SearchDevicesItemResp,
        SearchDevicesResp, Summary,
    },
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp, Value,
};

use uuid::Uuid;

pub mod coap;
pub mod modbus;
pub mod opcua;

#[async_trait]
pub trait Device: Send + Sync {
    fn get_id(&self) -> &Uuid;
    async fn search(&self) -> SearchDevicesItemResp;
    async fn update(&mut self, device_conf: DeviceConf) -> HaliaResult<()>;
    async fn start(&mut self) -> HaliaResult<()>;
    async fn stop(&mut self) -> HaliaResult<()>;
    async fn delete(&mut self) -> HaliaResult<()>;

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn search_sources(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp;
    async fn update_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn write_source_value(&mut self, source_id: Uuid, req: Value) -> HaliaResult<()>;
    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()>;

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn search_sinks(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp;
    async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()>;
    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()>;

    async fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>>;
    async fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;

    async fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>>;
    async fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
}

pub static GLOBAL_DEVICE_MANAGER: LazyLock<DeviceManager> = LazyLock::new(|| DeviceManager {
    devices: RwLock::new(vec![]),
});

pub struct DeviceManager {
    devices: RwLock<Vec<Box<dyn Device>>>,
}

macro_rules! device_not_found_err {
    () => {
        Err(HaliaError::NotFound("设备".to_owned()))
    };
}

#[macro_export]
macro_rules! source_not_found_err {
    () => {
        Err(HaliaError::NotFound("源".to_owned()))
    };
}

#[macro_export]
macro_rules! sink_not_found_err {
    () => {
        Err(HaliaError::NotFound("动作".to_owned()))
    };
}

impl DeviceManager {
    //     pub fn check_duplicate_name(&self, device_id: &Option<Uuid>, name: &str) -> HaliaResult<()> {
    //         GLOBAL_MODBUS_MANAGER.check_duplicate_name(device_id, name)?;
    //         GLOBAL_COAP_MANAGER.check_duplicate_name(device_id, name)?;
    //         GLOBAL_OPCUA_MANAGER.check_duplicate_name(device_id, name)?;
    //         Ok(())
    //     }

    //     pub async fn delete(&self, device_id: &Uuid) {
    //         self.devices.write().await.retain(|(_, id)| id != device_id);
    //     }

    pub async fn recover(&self) -> HaliaResult<()> {
        match persistence::read_devices().await {
            Ok(datas) => {
                for data in datas {
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 3);

                    let device_id = Uuid::from_str(items[0]).unwrap();

                    let req: CreateUpdateDeviceReq = serde_json::from_str(items[2])?;
                    self.create_device(device_id, req, true).await?;
                    match items[1] {
                        "0" => {}
                        "1" => self.start_device(device_id).await.unwrap(),
                        _ => panic!("文件已损坏"),
                    }
                }
                Ok(())
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => match persistence::init_devices().await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.into()),
                },
                _ => Err(e.into()),
            },
        }
    }
}

impl DeviceManager {
    pub async fn get_summary(&self) -> Summary {
        let mut total = 0;
        let mut running_cnt = 0;
        let mut err_cnt = 0;
        let mut off_cnt = 0;
        for device in self.devices.read().await.iter().rev() {
            let device = device.search().await;
            total += 1;

            if device.err.is_some() {
                err_cnt += 1;
            } else {
                if device.on {
                    running_cnt += 1;
                } else {
                    off_cnt += 1;
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

    pub async fn create_device(
        &self,
        device_id: Uuid,
        req: CreateUpdateDeviceReq,
        recover: bool,
    ) -> HaliaResult<()> {
        let data = serde_json::to_string(&req)?;
        let device = match req.typ {
            DeviceType::Modbus => modbus::new(device_id, req.conf).await?,
            DeviceType::Opcua => opcua::new(device_id, req.conf).await?,
            DeviceType::Coap => coap::new(device_id, req.conf).await?,
        };
        if !recover {
            persistence::create_device(device.get_id(), &data).await?;
        }
        self.devices.write().await.push(device);
        Ok(())
    }

    pub async fn search_devices(
        &self,
        pagination: Pagination,
        query_params: QueryParams,
    ) -> SearchDevicesResp {
        let mut data = vec![];
        let mut total = 0;

        for device in self.devices.read().await.iter().rev() {
            let device = device.search().await;
            if let Some(typ) = &query_params.typ {
                if *typ != device.typ {
                    continue;
                }
            }

            if let Some(name) = &query_params.name {
                if !device.conf.base.name.contains(name) {
                    continue;
                }
            }

            if let Some(on) = &query_params.on {
                if device.on != *on {
                    continue;
                }
            }

            if let Some(err) = &query_params.err {
                if device.err.is_some() != *err {
                    continue;
                }
            }

            if total >= (pagination.page - 1) * pagination.size
                && total < pagination.page * pagination.size
            {
                data.push(device);
            }
            total += 1;
        }

        SearchDevicesResp { total, data }
    }

    pub async fn update_device(
        &self,
        device_id: Uuid,
        req: CreateUpdateDeviceReq,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => {
                let data = serde_json::to_string(&req)?;
                device.update(req.conf).await?;
                persistence::update_device_conf(device.get_id(), &data).await?;
                Ok(())
            }
            None => device_not_found_err!(),
        }
    }

    pub async fn start_device(&self, device_id: Uuid) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => {
                device.start().await?;
                persistence::update_device_status(device.get_id(), persistence::Status::Runing)
                    .await?;
                Ok(())
            }
            None => device_not_found_err!(),
        }
    }

    pub async fn stop_device(&self, device_id: Uuid) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => {
                device.stop().await?;
                persistence::update_device_status(device.get_id(), persistence::Status::Stopped)
                    .await?;
                Ok(())
            }
            None => device_not_found_err!(),
        }
    }

    pub async fn delete_device(&self, device_id: Uuid) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => {
                device.delete().await?;
                persistence::delete_device(device.get_id()).await?;
            }
            None => return device_not_found_err!(),
        }

        self.devices
            .write()
            .await
            .retain(|device| *device.get_id() != device_id);

        Ok(())
    }
}

impl DeviceManager {
    pub async fn create_source(
        &self,
        device_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => {
                let source_id = Uuid::new_v4();
                let data = serde_json::to_string(&req)?;
                device.create_source(source_id.clone(), req).await?;
                persistence::create_source(device.get_id(), &source_id, &data).await?;
                Ok(())
            }

            None => device_not_found_err!(),
        }
    }

    pub async fn search_sources(
        &self,
        device_id: Uuid,
        pagination: Pagination,
        query: QueryParams,
    ) -> HaliaResult<SearchSourcesOrSinksResp> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => Ok(device.search_sources(pagination, query).await),
            None => device_not_found_err!(),
        }
    }

    pub async fn update_source(
        &self,
        device_id: Uuid,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => {
                let data = serde_json::to_string(&req)?;
                device.update_source(source_id, req).await?;
                persistence::update_source(device.get_id(), &source_id, &data).await?;
                Ok(())
            }
            None => device_not_found_err!(),
        }
    }

    pub async fn write_source_value(
        &self,
        device_id: Uuid,
        source_id: Uuid,
        req: Value,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => device.write_source_value(source_id, req).await,
            None => device_not_found_err!(),
        }
    }

    pub async fn delete_source(&self, device_id: Uuid, source_id: Uuid) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => {
                device.delete_source(source_id).await?;
                persistence::delete_source(device.get_id(), &source_id).await?;
                Ok(())
            }
            None => device_not_found_err!(),
        }
    }
}

impl DeviceManager {
    pub async fn create_sink(
        &self,
        device_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => {
                let data = serde_json::to_string(&req)?;
                let sink_id = Uuid::new_v4();
                device.create_sink(sink_id.clone(), req).await?;
                persistence::create_sink(device.get_id(), &sink_id, &data).await?;
                Ok(())
            }
            None => device_not_found_err!(),
        }
    }

    pub async fn search_sinks(
        &self,
        device_id: Uuid,
        pagination: Pagination,
        query: QueryParams,
    ) -> HaliaResult<SearchSourcesOrSinksResp> {
        match self
            .devices
            .read()
            .await
            .iter()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => Ok(device.search_sinks(pagination, query).await),
            None => device_not_found_err!(),
        }
    }

    pub async fn update_sink(
        &self,
        device_id: Uuid,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => device.update_sink(sink_id, req).await,
            None => device_not_found_err!(),
        }
    }

    pub async fn delete_sink(&self, device_id: Uuid, sink_id: Uuid) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == device_id)
        {
            Some(device) => device.delete_sink(sink_id).await,
            None => device_not_found_err!(),
        }
    }
}

impl DeviceManager {
    pub async fn add_source_ref(
        &self,
        device_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == *device_id)
        {
            Some(device) => device.add_source_ref(source_id, rule_id).await,
            None => device_not_found_err!(),
        }
    }

    pub async fn get_source_rx(
        &self,
        device_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == *device_id)
        {
            Some(device) => device.get_source_rx(source_id, rule_id).await,
            None => device_not_found_err!(),
        }
    }

    pub async fn del_source_rx(
        &self,
        device_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == *device_id)
        {
            Some(device) => device.del_source_rx(source_id, rule_id).await,
            None => device_not_found_err!(),
        }
    }

    pub async fn del_source_ref(
        &self,
        device_id: &Uuid,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == *device_id)
        {
            Some(device) => device.del_source_ref(source_id, rule_id).await,
            None => device_not_found_err!(),
        }
    }

    pub async fn add_sink_ref(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == *device_id)
        {
            Some(device) => device.add_sink_ref(sink_id, rule_id).await,
            None => device_not_found_err!(),
        }
    }

    pub async fn get_sink_tx(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == *device_id)
        {
            Some(device) => device.get_sink_tx(sink_id, rule_id).await,
            None => device_not_found_err!(),
        }
    }

    pub async fn del_sink_tx(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == *device_id)
        {
            Some(device) => device.del_sink_tx(sink_id, rule_id).await,
            None => device_not_found_err!(),
        }
    }

    pub async fn del_sink_ref(
        &self,
        device_id: &Uuid,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<()> {
        match self
            .devices
            .write()
            .await
            .iter_mut()
            .find(|device| *device.get_id() == *device_id)
        {
            Some(device) => device.del_sink_ref(sink_id, rule_id).await,
            None => device_not_found_err!(),
        }
    }
}
