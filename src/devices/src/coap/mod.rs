use anyhow::Result;
use api::API;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    get_id,
    persistence::{self, Status},
};
use message::MessageBatch;
use protocol::coap::request::CoapOption;
use sink::Sink;
use std::str::FromStr;
use tokio::sync::{broadcast, mpsc};
use types::{
    devices::{
        coap::{
            CreateUpdateAPIReq, CreateUpdateCoapReq, CreateUpdateSinkReq, SearchAPIsResp,
            SearchSinksResp,
        },
        DeviceType, SearchDevicesItemConf, SearchDevicesItemResp,
    },
    Pagination,
};
use uuid::Uuid;

mod api;
pub mod manager;
mod observe;
mod sink;

macro_rules! api_not_found_err {
    ($api_id:expr) => {
        Err(HaliaError::NotFound("coap设备API".to_owned(), $api_id))
    };
}

macro_rules! sink_not_found_err {
    ($sink_id:expr) => {
        Err(HaliaError::NotFound("coap设备动作".to_owned(), $sink_id))
    };
}

pub struct Coap {
    id: Uuid,
    conf: CreateUpdateCoapReq,

    apis: Vec<API>,
    sinks: Vec<Sink>,

    on: bool,
    err: Option<String>,
}

impl Coap {
    pub async fn new(device_id: Option<Uuid>, req: CreateUpdateCoapReq) -> HaliaResult<Self> {
        let (device_id, new) = get_id(device_id);

        if new {
            persistence::devices::coap::create(&device_id, serde_json::to_string(&req).unwrap())
                .await?;
        }

        Ok(Coap {
            id: device_id,
            conf: req,
            apis: vec![],
            sinks: vec![],
            on: false,
            err: None,
        })
    }

    async fn recover(&mut self) -> HaliaResult<()> {
        let api_datas = persistence::devices::coap::read_apis(&self.id).await?;
        for api_data in api_datas {
            if api_data.len() == 0 {
                continue;
            }
            let items = api_data
                .split(persistence::DELIMITER)
                .collect::<Vec<&str>>();
            assert_eq!(items.len(), 2);

            let api_id = Uuid::from_str(items[0]).unwrap();
            let req: CreateUpdateAPIReq = serde_json::from_str(items[1])?;
            self.create_api(Some(api_id), req).await?;
        }

        Ok(())
    }

    pub fn check_duplicate_name(&self, device_id: &Option<Uuid>, name: &str) -> bool {
        if let Some(device_id) = device_id {
            if *device_id == self.id {
                return false;
            }
        }

        self.conf.base.name == name
    }

    pub fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id.clone(),
            typ: DeviceType::Coap,
            on: self.on,
            err: self.err.clone(),
            rtt: 999,
            conf: SearchDevicesItemConf {
                base: self.conf.base.clone(),
                ext: serde_json::json!(self.conf.ext),
            },
        }
    }

    pub async fn update(&mut self, req: CreateUpdateCoapReq) -> HaliaResult<()> {
        persistence::devices::update_device_conf(&self.id, serde_json::to_string(&req).unwrap())
            .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }

        self.conf = req;

        if restart && self.on {
            for api in self.apis.iter_mut() {
                _ = api.restart(&self.conf.ext).await;
            }

            for sink in self.sinks.iter_mut() {
                _ = sink.restart(&self.conf.ext).await;
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;

        for api in self.apis.iter_mut() {
            _ = api.start(&self.conf.ext).await;
        }

        for sink in self.sinks.iter_mut() {
            _ = sink.start(&self.conf.ext).await;
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if self.apis.iter().any(|api| !api.ref_info.can_stop()) {
            return Err(HaliaError::Common("有api正被引用中".to_owned()));
        }
        if self.sinks.iter().any(|sink| sink.ref_info.can_stop()) {
            return Err(HaliaError::Common("有动作正被引用中".to_owned()));
        }

        check_and_set_on_false!(self);

        for api in self.apis.iter_mut() {
            _ = api.stop().await;
        }

        for sink in self.sinks.iter_mut() {
            _ = sink.stop().await;
        }

        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if self.on {
            return Err(HaliaError::Running);
        }

        if self.apis.iter().any(|api| !api.ref_info.can_delete()) {
            return Err(HaliaError::Common("有api正被引用中".to_owned()));
        }

        if self.sinks.iter().any(|sink| !sink.ref_info.can_delete()) {
            return Err(HaliaError::Common("有动作正被引用中".to_owned()));
        }

        persistence::devices::delete_device(&self.id).await?;

        Ok(())
    }

    pub async fn create_api(
        &mut self,
        api_id: Option<Uuid>,
        req: CreateUpdateAPIReq,
    ) -> HaliaResult<()> {
        let mut api = API::new(&self.id, api_id, req).await?;
        if self.on {
            _ = api.start(&self.conf.ext).await;
        }
        self.apis.push(api);

        Ok(())
    }

    pub async fn search_apis(&self, pagination: Pagination) -> SearchAPIsResp {
        let mut data = vec![];
        for api in self
            .apis
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(api.search());
        }

        SearchAPIsResp {
            total: self.apis.len(),
            data,
        }
    }

    pub async fn update_api(&mut self, api_id: Uuid, req: CreateUpdateAPIReq) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == api_id) {
            Some(api) => api.update(&self.id, req).await,
            None => api_not_found_err!(api_id),
        }
    }

    pub async fn delete_api(&mut self, api_id: Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == api_id) {
            Some(api) => api.delete(&self.id).await?,
            None => return api_not_found_err!(api_id),
        }
        self.apis.retain(|api| api.id != api_id);
        Ok(())
    }

    pub async fn add_api_ref(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.ref_info.add_ref(rule_id)),
            None => api_not_found_err!(api_id.clone()),
        }
    }

    pub async fn get_api_mb_rx(
        &mut self,
        api_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.get_mb_rx(rule_id)),
            None => api_not_found_err!(api_id.clone()),
        }
    }

    pub async fn del_api_ref(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.ref_info.del_ref(rule_id)),
            None => api_not_found_err!(api_id.clone()),
        }
    }

    pub async fn del_api_mb_rx(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.del_mb_rx(rule_id)),
            None => api_not_found_err!(api_id.clone()),
        }
    }

    pub async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(&self.id, sink_id, req).await {
            Ok(sink) => Ok(self.sinks.push(sink)),
            Err(e) => Err(e),
        }
    }

    pub async fn search_sinks(&self, pagination: Pagination) -> SearchSinksResp {
        let mut data = vec![];
        for sink in self
            .sinks
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(sink.search());
            if data.len() == pagination.size {
                break;
            }
        }

        SearchSinksResp {
            total: self.sinks.len(),
            data,
        }
    }

    pub async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => sink.update(&self.id, req).await,
            None => sink_not_found_err!(sink_id),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id != sink_id);
                Ok(())
            }
            None => sink_not_found_err!(sink_id),
        }
    }

    pub fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.add_ref(rule_id)),
            None => sink_not_found_err!(sink_id.clone()),
        }
    }

    pub async fn get_sink_mb_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.get_mb_tx(rule_id)),
            None => sink_not_found_err!(sink_id.clone()),
        }
    }

    pub async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.del_ref(rule_id)),
            None => sink_not_found_err!(sink_id.clone()),
        }
    }

    pub async fn del_sink_mb_rx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.del_mb_tx(rule_id)),
            None => sink_not_found_err!(sink_id.clone()),
        }
    }
}

pub(crate) fn transform_options(
    input_options: &Vec<(types::devices::coap::CoapOption, String)>,
) -> Result<Vec<(CoapOption, Vec<u8>)>> {
    let mut options = vec![];
    for (k, v) in input_options {
        let v = BASE64_STANDARD.decode(&v)?;
        match k {
            types::devices::coap::CoapOption::IfMatch => options.push((CoapOption::IfMatch, v)),
            types::devices::coap::CoapOption::UriHost => options.push((CoapOption::UriHost, v)),
            types::devices::coap::CoapOption::ETag => options.push((CoapOption::ETag, v)),
            types::devices::coap::CoapOption::IfNoneMatch => {
                options.push((CoapOption::IfNoneMatch, v))
            }
            types::devices::coap::CoapOption::Observe => options.push((CoapOption::Observe, v)),
            types::devices::coap::CoapOption::UriPort => options.push((CoapOption::UriPort, v)),
            types::devices::coap::CoapOption::LocationPath => {
                options.push((CoapOption::LocationPath, v))
            }
            types::devices::coap::CoapOption::Oscore => options.push((CoapOption::Oscore, v)),
            types::devices::coap::CoapOption::UriPath => options.push((CoapOption::UriPath, v)),
            types::devices::coap::CoapOption::ContentFormat => {
                options.push((CoapOption::ContentFormat, v))
            }
            types::devices::coap::CoapOption::MaxAge => options.push((CoapOption::MaxAge, v)),
            types::devices::coap::CoapOption::UriQuery => options.push((CoapOption::UriQuery, v)),
            types::devices::coap::CoapOption::Accept => options.push((CoapOption::Accept, v)),
            types::devices::coap::CoapOption::LocationQuery => {
                options.push((CoapOption::LocationQuery, v))
            }
            types::devices::coap::CoapOption::Block2 => options.push((CoapOption::Block2, v)),
            types::devices::coap::CoapOption::Block1 => options.push((CoapOption::Block1, v)),
            types::devices::coap::CoapOption::ProxyUri => options.push((CoapOption::ProxyUri, v)),
            types::devices::coap::CoapOption::ProxyScheme => {
                options.push((CoapOption::ProxyScheme, v))
            }
            types::devices::coap::CoapOption::Size1 => options.push((CoapOption::Size1, v)),
            types::devices::coap::CoapOption::Size2 => options.push((CoapOption::Size2, v)),
            types::devices::coap::CoapOption::NoResponse => {
                options.push((CoapOption::NoResponse, v))
            }
        }
    }
    Ok(options)
}
