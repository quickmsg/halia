use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{local::Local, Persistence},
};
use message::MessageBatch;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
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
    fn check_duplicate(&self, req: &CreateUpdateDeviceReq) -> HaliaResult<()>;
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

    fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>>;
    fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;

    fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>>;
    fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
    fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()>;
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

pub async fn load_from_persistence(
    persistence: &Arc<Mutex<Local>>,
) -> HaliaResult<Arc<RwLock<Vec<Box<dyn Device>>>>> {
    let db_devices = persistence.lock().await.read_devices()?;
    let devices: Arc<RwLock<Vec<Box<dyn Device>>>> = Arc::new(RwLock::new(vec![]));
    for db_device in db_devices {
        let device_id = Uuid::from_str(&db_device.id).unwrap();

        let db_sources = persistence.lock().await.read_sources(&device_id).unwrap();
        let db_sinks = persistence.lock().await.read_sinks(&device_id).unwrap();
        create_device(persistence, &devices, device_id, db_device.conf, false)
            .await
            .unwrap();

        for db_source in db_sources {
            create_source(
                persistence,
                &devices,
                device_id,
                Uuid::from_str(&db_source.id).unwrap(),
                db_source.conf,
                false,
            )
            .await?;
        }

        for db_sink in db_sinks {
            create_sink(
                persistence,
                &devices,
                device_id,
                Uuid::from_str(&db_sink.id).unwrap(),
                db_sink.conf,
                false,
            )
            .await?;
        }

        if db_device.status == 1 {
            start_device(persistence, &devices, device_id).await?;
        }
    }

    Ok(devices)
}

pub async fn get_summary(devices: &Arc<RwLock<Vec<Box<dyn Device>>>>) -> Summary {
    let mut total = 0;
    let mut running_cnt = 0;
    let mut err_cnt = 0;
    let mut off_cnt = 0;
    for device in devices.read().await.iter().rev() {
        let device = device.search().await;
        total += 1;

        if device.common.err.is_some() {
            err_cnt += 1;
        } else {
            if device.common.on {
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
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateDeviceReq = serde_json::from_str(&body)?;
    let device = match req.device_type {
        DeviceType::Modbus => modbus::new(device_id, req.conf)?,
        DeviceType::Opcua => opcua::new(device_id, req.conf).await?,
        DeviceType::Coap => coap::new(device_id, req.conf).await?,
    };
    devices.write().await.push(device);
    if persist {
        persistence.lock().await.create_device(&device_id, body)?;
    }
    Ok(())
}

pub async fn search_devices(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    pagination: Pagination,
    query_params: QueryParams,
) -> SearchDevicesResp {
    let mut data = vec![];
    let mut total = 0;

    for device in devices.read().await.iter().rev() {
        let device = device.search().await;
        if let Some(device_type) = &query_params.device_type {
            if *device_type != device.common.device_type {
                continue;
            }
        }

        if let Some(name) = &query_params.name {
            if !device.conf.base.name.contains(name) {
                continue;
            }
        }

        if let Some(on) = &query_params.on {
            if device.common.on != *on {
                continue;
            }
        }

        if let Some(err) = &query_params.err {
            if device.common.err.is_some() != *err {
                continue;
            }
        }

        if pagination.check(total) {
            data.push(device);
        }

        total += 1;
    }

    SearchDevicesResp { total, data }
}

pub async fn update_device(
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateDeviceReq = serde_json::from_str(&body)?;

    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.update(req.conf).await?,
        None => return device_not_found_err!(),
    }

    persistence
        .lock()
        .await
        .update_device_conf(&device_id, body)?;

    Ok(())
}

pub async fn start_device(
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.start().await?,
        None => return device_not_found_err!(),
    }

    persistence
        .lock()
        .await
        .update_device_status(&device_id, true)?;
    Ok(())
}

pub async fn stop_device(
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.stop().await?,
        None => return device_not_found_err!(),
    }

    persistence
        .lock()
        .await
        .update_device_status(&device_id, false)?;

    Ok(())
}

pub async fn delete_device(
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.delete().await?,
        None => return device_not_found_err!(),
    }

    devices
        .write()
        .await
        .retain(|device| *device.get_id() != device_id);
    persistence.lock().await.delete_device(&device_id)?;

    Ok(())
}

pub async fn create_source(
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    source_id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.create_source(source_id.clone(), req).await?,
        None => return device_not_found_err!(),
    }

    if persist {
        persistence
            .lock()
            .await
            .create_source(&device_id, &source_id, body)?;
    }

    Ok(())
}

pub async fn search_sources(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    match devices
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
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    source_id: Uuid,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.update_source(source_id, req).await?,
        None => return device_not_found_err!(),
    }

    persistence.lock().await.update_source(&source_id, body)?;

    Ok(())
}

pub async fn write_source_value(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    source_id: Uuid,
    req: Value,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.write_source_value(source_id, req).await,
        None => device_not_found_err!(),
    }
}

pub async fn delete_source(
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    source_id: Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.delete_source(source_id).await?,
        None => return device_not_found_err!(),
    }

    persistence.lock().await.delete_source(&source_id)?;

    Ok(())
}

pub async fn add_source_ref(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == *device_id)
    {
        Some(device) => device.add_source_ref(source_id, rule_id),
        None => device_not_found_err!(),
    }
}

pub async fn get_source_rx(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
    match devices
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
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == *device_id)
    {
        Some(device) => device.del_source_rx(source_id, rule_id),
        None => device_not_found_err!(),
    }
}

pub async fn del_source_ref(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: &Uuid,
    source_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == *device_id)
    {
        Some(device) => device.del_source_ref(source_id, rule_id),
        None => device_not_found_err!(),
    }
}

pub async fn create_sink(
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    sink_id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.create_sink(sink_id, req).await?,
        None => return device_not_found_err!(),
    }

    if persist {
        persistence
            .lock()
            .await
            .create_sink(&device_id, &sink_id, body)?;
    }

    Ok(())
}

pub async fn search_sinks(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    pagination: Pagination,
    query: QueryParams,
) -> HaliaResult<SearchSourcesOrSinksResp> {
    match devices
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
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    sink_id: Uuid,
    body: String,
) -> HaliaResult<()> {
    let req: CreateUpdateSourceOrSinkReq = serde_json::from_str(&body)?;
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.update_sink(sink_id, req).await?,
        None => return device_not_found_err!(),
    }

    persistence.lock().await.update_sink(&sink_id, body)?;

    Ok(())
}

pub async fn delete_sink(
    persistence: &Arc<Mutex<Local>>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    sink_id: Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == device_id)
    {
        Some(device) => device.delete_sink(sink_id).await?,
        None => return device_not_found_err!(),
    }

    persistence.lock().await.delete_sink(&sink_id)?;

    Ok(())
}

pub async fn add_sink_ref(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == *device_id)
    {
        Some(device) => device.add_sink_ref(sink_id, rule_id),
        None => device_not_found_err!(),
    }
}

pub async fn get_sink_tx(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<mpsc::Sender<MessageBatch>> {
    match devices
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
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == *device_id)
    {
        Some(device) => device.del_sink_tx(sink_id, rule_id),
        None => device_not_found_err!(),
    }
}

pub async fn del_sink_ref(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: &Uuid,
    sink_id: &Uuid,
    rule_id: &Uuid,
) -> HaliaResult<()> {
    match devices
        .write()
        .await
        .iter_mut()
        .find(|device| *device.get_id() == *device_id)
    {
        Some(device) => device.del_sink_ref(sink_id, rule_id),
        None => device_not_found_err!(),
    }
}
