use std::{str::FromStr, sync::Arc};

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use sqlx::AnyPool;
use tokio::sync::{broadcast, mpsc, RwLock};
use types::{
    devices::{
        CreateUpdateDeviceReq, DeviceConf, DeviceType, QueryParams, QueryRuleInfo,
        SearchDevicesItemResp, SearchDevicesResp, SearchRuleInfo, Summary,
    },
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksInfoResp,
    SearchSourcesOrSinksResp, Value,
};

use uuid::Uuid;

pub mod coap;
pub mod modbus;
pub mod opcua;

#[async_trait]
pub trait Device: Send + Sync {
    fn get_id(&self) -> &Uuid;
    fn check_duplicate(&self, req: &CreateUpdateDeviceReq) -> HaliaResult<()>;
    async fn read(&self) -> SearchDevicesItemResp;
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
    async fn read_source(&self, source_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp>;
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
    async fn read_sink(&self, sink_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp>;
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

pub async fn load_from_persistence(
    persistence: &Arc<AnyPool>,
) -> HaliaResult<Arc<RwLock<Vec<Box<dyn Device>>>>> {
    let db_devices = persistence::device::read_devices(&persistence).await?;

    let devices: Arc<RwLock<Vec<Box<dyn Device>>>> = Arc::new(RwLock::new(vec![]));
    for db_device in db_devices {
        let device_id = Uuid::from_str(&db_device.id).unwrap();

        let db_sources = persistence::source::read_sources(&persistence, &device_id).await?;
        let db_sinks = persistence::sink::read_sinks(&persistence, &device_id).await?;

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
        let device = device.read().await;
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

pub async fn get_rule_info(
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    query: QueryRuleInfo,
) -> HaliaResult<SearchRuleInfo> {
    match devices
        .read()
        .await
        .iter()
        .find(|device| *device.get_id() == query.device_id)
    {
        Some(device) => {
            let device_info = device.read().await;
            match (query.source_id, query.sink_id) {
                (Some(source_id), None) => {
                    let source_info = device.read_source(&source_id).await?;
                    Ok(SearchRuleInfo {
                        device: device_info,
                        source: Some(source_info),
                        sink: None,
                    })
                }
                (None, Some(sink_id)) => {
                    let sink_info = device.read_sink(&sink_id).await?;
                    Ok(SearchRuleInfo {
                        device: device_info,
                        source: None,
                        sink: Some(sink_info),
                    })
                }
                _ => {
                    return Err(HaliaError::Common(
                        "查询source_id或sink_id参数错误！".to_string(),
                    ))
                }
            }
        }
        None => Err(HaliaError::NotFound),
    }
}

pub async fn create_device(
    persistence: &Arc<AnyPool>,
    devices: &Arc<RwLock<Vec<Box<dyn Device>>>>,
    device_id: Uuid,
    body: String,
    persist: bool,
) -> HaliaResult<()> {
    let req: CreateUpdateDeviceReq = serde_json::from_str(&body)?;
    for device in devices.read().await.iter() {
        device.check_duplicate(&req)?;
    }

    let device = match req.device_type {
        DeviceType::Modbus => modbus::new(device_id, req.conf)?,
        DeviceType::Opcua => opcua::new(device_id, req.conf).await?,
        DeviceType::Coap => coap::new(device_id, req.conf).await?,
    };
    devices.write().await.push(device);
    if persist {
        persistence::device::create_device(&persistence, &device_id, body).await?;
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
        let device = device.read().await;
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
    persistence: &Arc<AnyPool>,
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
        None => return Err(HaliaError::NotFound),
    }

    persistence::device::update_device_conf(persistence, &device_id, body).await?;

    Ok(())
}

pub async fn start_device(
    persistence: &Arc<AnyPool>,
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
        None => return Err(HaliaError::NotFound),
    }

    persistence::device::update_device_status(persistence, &device_id, true).await?;
    Ok(())
}

pub async fn stop_device(
    persistence: &Arc<AnyPool>,
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
        None => return Err(HaliaError::NotFound),
    }

    persistence::device::update_device_status(persistence, &device_id, false).await?;

    Ok(())
}

pub async fn delete_device(
    persistence: &Arc<AnyPool>,
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
        None => return Err(HaliaError::NotFound),
    }

    devices
        .write()
        .await
        .retain(|device| *device.get_id() != device_id);
    persistence::device::delete_device(persistence, &device_id).await?;

    Ok(())
}

pub async fn create_source(
    persistence: &Arc<AnyPool>,
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
        None => return Err(HaliaError::NotFound),
    }

    if persist {
        persistence::source::create_source(persistence, &device_id, &source_id, body).await?;
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
        None => Err(HaliaError::NotFound),
    }
}

pub async fn update_source(
    persistence: &Arc<AnyPool>,
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
        None => return Err(HaliaError::NotFound),
    }

    persistence::source::update_source(persistence, &source_id, body).await?;

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
        None => Err(HaliaError::NotFound),
    }
}

pub async fn delete_source(
    persistence: &Arc<AnyPool>,
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
        None => return Err(HaliaError::NotFound),
    }

    persistence::source::delete_source(persistence, &source_id).await?;

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
        None => Err(HaliaError::NotFound),
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
        None => Err(HaliaError::NotFound),
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
        None => Err(HaliaError::NotFound),
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
        None => Err(HaliaError::NotFound),
    }
}

pub async fn create_sink(
    persistence: &Arc<AnyPool>,
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
        None => return Err(HaliaError::NotFound),
    }

    if persist {
        persistence::sink::create_sink(persistence, &device_id, &sink_id, body).await?;
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
        None => Err(HaliaError::NotFound),
    }
}

pub async fn update_sink(
    pool: &Arc<AnyPool>,
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
        None => return Err(HaliaError::NotFound),
    }

    persistence::sink::update_sink(pool, &sink_id, body).await?;

    Ok(())
}

pub async fn delete_sink(
    pool: &Arc<AnyPool>,
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
        None => return Err(HaliaError::NotFound),
    }

    persistence::sink::delete_sink(pool, &sink_id).await?;

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
        None => Err(HaliaError::NotFound),
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
        None => Err(HaliaError::NotFound),
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
        None => Err(HaliaError::NotFound),
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
        None => Err(HaliaError::NotFound),
    }
}
