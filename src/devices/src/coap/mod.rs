use anyhow::Result;
use api::API;
use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    get_id,
    persistence::{self, Status},
};
use message::MessageBatch;
use observe::Observe;
use protocol::coap::request::CoapOption;
use sink::Sink;
use std::str::FromStr;
use tokio::sync::{broadcast, mpsc};
use tracing::warn;
use types::{
    devices::{
        coap::{
            CreateUpdateAPIReq, CreateUpdateCoapReq, CreateUpdateObserveReq, CreateUpdateSinkReq,
            QueryObserves, SearchAPIsResp, SearchObservesResp, SearchSinksResp,
        },
        CreateUpdateDeviceReq, DeviceType, QueryParams, SearchDevicesItemConf,
        SearchDevicesItemResp,
    },
    CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp, Value,
};
use uuid::Uuid;

use crate::{sink_not_found_err, source_not_found_err, Device};

// use crate::Device;

mod api;
// pub mod manager;
mod observe;
mod sink;
mod source;

struct Coap {
    id: Uuid,
    conf: CreateUpdateCoapReq,

    observes: Vec<Observe>,
    apis: Vec<API>,
    sinks: Vec<Sink>,

    on: bool,
    err: Option<String>,
}

pub async fn new(
    device_id: Option<Uuid>,
    req: CreateUpdateDeviceReq,
) -> HaliaResult<Box<dyn Device>> {
    // Self::check_conf(&req)?;

    let (device_id, new) = get_id(device_id);
    // if new {
    //     persistence::create_device(&device_id, serde_json::to_string(&req).unwrap())
    //         .await?;
    // }

    Ok(Box::new(Coap {
        id: device_id,
        conf: todo!(),
        observes: vec![],
        apis: vec![],
        sinks: vec![],
        on: false,
        err: None,
    }))
}

impl Coap {
    fn check_conf(_req: &CreateUpdateCoapReq) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateCoapReq) -> HaliaResult<()> {
        if self.conf.ext == req.ext {
            return Err(HaliaError::AddressExists);
        }

        Ok(())
    }

    async fn recover(&mut self) -> HaliaResult<()> {
        let observe_datas = persistence::read_sources(&self.id).await?;
        for observe_data in observe_datas {
            let items = observe_data
                .split(persistence::DELIMITER)
                .collect::<Vec<&str>>();
            assert_eq!(items.len(), 2);

            let observe_id = Uuid::from_str(items[0]).unwrap();
            let req: CreateUpdateObserveReq = serde_json::from_str(items[1])?;
            self.create_observe(Some(observe_id), req).await?;
        }

        // let api_datas = persistence::devices::coap::read_apis(&self.id).await?;
        // for api_data in api_datas {
        //     let items = api_data
        //         .split(persistence::DELIMITER)
        //         .collect::<Vec<&str>>();
        //     assert_eq!(items.len(), 2);

        //     let api_id = Uuid::from_str(items[0]).unwrap();
        //     let req: CreateUpdateAPIReq = serde_json::from_str(items[1])?;
        //     self.create_api(Some(api_id), req).await?;
        // }

        let sink_datas = persistence::read_sinks(&self.id).await?;
        for sink_data in sink_datas {
            let items = sink_data
                .split(persistence::DELIMITER)
                .collect::<Vec<&str>>();
            assert_eq!(items.len(), 2);

            let sink_id = Uuid::from_str(items[0]).unwrap();
            let req: CreateUpdateSinkReq = serde_json::from_str(items[1])?;
            self.create_sink(Some(sink_id), req).await?;
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
        // persistence::update_device_conf(&self.id, serde_json::to_string(&req).unwrap()).await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }

        self.conf = req;

        if restart && self.on {
            for observe in self.observes.iter_mut() {
                _ = observe.restart(&self.conf.ext).await;
            }
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

        persistence::update_device_status(&self.id, Status::Runing).await?;

        for observe in self.observes.iter_mut() {
            if let Err(e) = observe.start(&self.conf.ext).await {
                warn!("observe start err:{}", e);
            }
        }

        for api in self.apis.iter_mut() {
            _ = api.start(&self.conf.ext).await;
        }

        for sink in self.sinks.iter_mut() {
            _ = sink.start(&self.conf.ext).await;
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if self
            .observes
            .iter()
            .any(|observe| !observe.ref_info.can_stop())
        {
            return Err(HaliaError::Common("有观察器正被引用中".to_owned()));
        }
        if self.apis.iter().any(|api| !api.ref_info.can_stop()) {
            return Err(HaliaError::Common("有api正被引用中".to_owned()));
        }
        if self.sinks.iter().any(|sink| sink.ref_info.can_stop()) {
            return Err(HaliaError::Common("有动作正被引用中".to_owned()));
        }

        check_and_set_on_false!(self);

        for observe in self.observes.iter_mut() {
            _ = observe.stop().await;
        }
        for api in self.apis.iter_mut() {
            _ = api.stop().await;
        }
        for sink in self.sinks.iter_mut() {
            _ = sink.stop().await;
        }

        persistence::update_device_status(&self.id, Status::Stopped).await?;

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if self.on {
            return Err(HaliaError::Running);
        }

        if self
            .observes
            .iter()
            .any(|observe| !observe.ref_info.can_delete())
        {
            return Err(HaliaError::Common("有观察器正被引用中".to_owned()));
        }
        if self.apis.iter().any(|api| !api.ref_info.can_delete()) {
            return Err(HaliaError::Common("有api正被引用中".to_owned()));
        }
        if self.sinks.iter().any(|sink| !sink.ref_info.can_delete()) {
            return Err(HaliaError::Common("有动作正被引用中".to_owned()));
        }

        persistence::delete_device(&self.id).await?;

        Ok(())
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

// observe 代码块
impl Coap {
    pub async fn create_observe(
        &mut self,
        observe_id: Option<Uuid>,
        req: CreateUpdateObserveReq,
    ) -> HaliaResult<()> {
        for observe in self.observes.iter() {
            observe.check_duplicate(&req)?;
        }

        let mut observe = Observe::new(&self.id, observe_id, req).await?;
        if self.on {
            _ = observe.start(&self.conf.ext).await;
        }
        self.observes.push(observe);

        Ok(())
    }

    pub async fn search_observes(
        &self,
        pagination: Pagination,
        query: QueryObserves,
    ) -> SearchObservesResp {
        let mut total = 0;
        let mut data = vec![];
        for observe in self.observes.iter().rev() {
            let observe = observe.search();

            if let Some(name) = &query.name {
                if !observe.conf.base.name.contains(name) {
                    continue;
                }
            }

            if total >= ((pagination.page - 1) * pagination.size)
                && total < (pagination.page * pagination.size)
            {
                data.push(observe);
            }
            total += 1;
        }

        SearchObservesResp { total, data }
    }

    pub async fn update_observe(
        &mut self,
        observe_id: Uuid,
        req: CreateUpdateObserveReq,
    ) -> HaliaResult<()> {
        match self
            .observes
            .iter_mut()
            .find(|observe| observe.id == observe_id)
        {
            Some(observe) => observe.update(&self.id, req, &self.conf.ext).await,
            None => source_not_found_err!(),
        }
    }

    pub async fn delete_observe(&mut self, observe_id: Uuid) -> HaliaResult<()> {
        match self
            .observes
            .iter_mut()
            .find(|observe| observe.id == observe_id)
        {
            Some(observe) => observe.delete(&self.id).await?,
            None => return source_not_found_err!(),
        }
        self.observes.retain(|observe| observe.id != observe_id);
        Ok(())
    }

    pub fn add_observe_ref(&mut self, observe_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .observes
            .iter_mut()
            .find(|observe| observe.id == *observe_id)
        {
            Some(observe) => Ok(observe.ref_info.add_ref(rule_id)),
            None => return source_not_found_err!(),
        }
    }

    pub fn get_observe_rx(
        &mut self,
        observe_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self
            .observes
            .iter_mut()
            .find(|observe| observe.id == *observe_id)
        {
            Some(observe) => Ok(observe.get_mb_rx(rule_id)),
            None => source_not_found_err!(),
        }
    }

    pub fn del_observe_rx(&mut self, observe_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .observes
            .iter_mut()
            .find(|observe| observe.id == *observe_id)
        {
            Some(observe) => Ok(observe.del_mb_rx(rule_id)),
            None => source_not_found_err!(),
        }
    }

    pub fn del_observe_ref(&mut self, observe_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .observes
            .iter_mut()
            .find(|observe| observe.id == *observe_id)
        {
            Some(observe) => Ok(observe.ref_info.del_ref(rule_id)),
            None => source_not_found_err!(),
        }
    }
}

impl Coap {
    pub async fn create_api(
        &mut self,
        api_id: Option<Uuid>,
        req: CreateUpdateAPIReq,
    ) -> HaliaResult<()> {
        for api in self.apis.iter() {
            api.check_duplicate(&req)?;
        }

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
        for api in self.apis.iter() {
            if api.id != api_id {
                api.check_duplicate(&req)?;
            }
        }

        match self.apis.iter_mut().find(|api| api.id == api_id) {
            Some(api) => api.update(&self.id, req).await,
            None => source_not_found_err!(),
        }
    }

    pub async fn delete_api(&mut self, api_id: Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == api_id) {
            Some(api) => api.delete(&self.id).await?,
            None => return source_not_found_err!(),
        }
        self.apis.retain(|api| api.id != api_id);
        Ok(())
    }

    pub async fn add_api_ref(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.ref_info.add_ref(rule_id)),
            None => source_not_found_err!(),
        }
    }

    pub async fn get_api_rx(
        &mut self,
        api_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.get_rx(rule_id)),
            None => source_not_found_err!(),
        }
    }

    pub async fn del_api_ref(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.ref_info.del_ref(rule_id)),
            None => source_not_found_err!(),
        }
    }

    pub async fn del_api_rx(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.del_rx(rule_id)),
            None => source_not_found_err!(),
        }
    }
}

// sink
impl Coap {
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
            None => sink_not_found_err!(),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id != sink_id);
                Ok(())
            }
            None => sink_not_found_err!(),
        }
    }

    pub fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.add_ref(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    pub async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.get_tx(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    pub async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.del_ref(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    pub async fn del_sink_rx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.del_tx(rule_id)),
            None => sink_not_found_err!(),
        }
    }
}

// // source
// impl Coap {
//     pub async fn create_source(
//         &mut self,
//         observe_id: Option<Uuid>,
//         req: CreateUpdateObserveReq,
//     ) -> HaliaResult<()> {
//         for observe in self.observes.iter() {
//             observe.check_duplicate(&req)?;
//         }

//         let mut observe = Observe::new(&self.id, observe_id, req).await?;
//         if self.on {
//             _ = observe.start(&self.conf.ext).await;
//         }
//         self.observes.push(observe);

//         Ok(())
//     }

//     pub async fn search_observes(
//         &self,
//         pagination: Pagination,
//         query: QueryObserves,
//     ) -> SearchObservesResp {
//         let mut total = 0;
//         let mut data = vec![];
//         for observe in self.observes.iter().rev() {
//             let observe = observe.search();

//             if let Some(name) = &query.name {
//                 if !observe.conf.base.name.contains(name) {
//                     continue;
//                 }
//             }

//             if total >= ((pagination.page - 1) * pagination.size)
//                 && total < (pagination.page * pagination.size)
//             {
//                 data.push(observe);
//             }
//             total += 1;
//         }

//         SearchObservesResp { total, data }
//     }

//     pub async fn update_observe(
//         &mut self,
//         observe_id: Uuid,
//         req: CreateUpdateObserveReq,
//     ) -> HaliaResult<()> {
//         match self
//             .observes
//             .iter_mut()
//             .find(|observe| observe.id == observe_id)
//         {
//             Some(observe) => observe.update(&self.id, req, &self.conf.ext).await,
//             None => observe_not_found_err!(observe_id),
//         }
//     }

//     pub async fn delete_observe(&mut self, observe_id: Uuid) -> HaliaResult<()> {
//         match self
//             .observes
//             .iter_mut()
//             .find(|observe| observe.id == observe_id)
//         {
//             Some(observe) => observe.delete(&self.id).await?,
//             None => return observe_not_found_err!(observe_id),
//         }
//         self.observes.retain(|observe| observe.id != observe_id);
//         Ok(())
//     }

//     pub fn add_observe_ref(&mut self, observe_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
//         match self
//             .observes
//             .iter_mut()
//             .find(|observe| observe.id == *observe_id)
//         {
//             Some(observe) => Ok(observe.ref_info.add_ref(rule_id)),
//             None => return observe_not_found_err!(observe_id.clone()),
//         }
//     }

//     pub fn get_observe_rx(
//         &mut self,
//         observe_id: &Uuid,
//         rule_id: &Uuid,
//     ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
//         match self
//             .observes
//             .iter_mut()
//             .find(|observe| observe.id == *observe_id)
//         {
//             Some(observe) => Ok(observe.get_mb_rx(rule_id)),
//             None => observe_not_found_err!(observe_id.clone()),
//         }
//     }

//     pub fn del_observe_rx(&mut self, observe_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
//         match self
//             .observes
//             .iter_mut()
//             .find(|observe| observe.id == *observe_id)
//         {
//             Some(observe) => Ok(observe.del_mb_rx(rule_id)),
//             None => observe_not_found_err!(observe_id.clone()),
//         }
//     }

//     pub fn del_observe_ref(&mut self, observe_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
//         match self
//             .observes
//             .iter_mut()
//             .find(|observe| observe.id == *observe_id)
//         {
//             Some(observe) => Ok(observe.ref_info.del_ref(rule_id)),
//             None => return observe_not_found_err!(observe_id.clone()),
//         }
//     }
// }

#[async_trait]
impl Device for Coap {
    fn get_id(&self) -> Uuid {
        self.id.clone()
    }

    async fn search(&self) -> SearchDevicesItemResp {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn update<'life0, 'async_trait>(
        &'life0 mut self,
        req: CreateUpdateDeviceReq,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn delete<'life0, 'async_trait>(
        &'life0 mut self,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn create_source<'life0, 'async_trait>(
        &'life0 mut self,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn search_sources<'life0, 'async_trait>(
        &'life0 self,
        pagination: Pagination,
        query: QueryParams,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = SearchSourcesOrSinksResp>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn update_source<'life0, 'async_trait>(
        &'life0 mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn delete_source<'life0, 'async_trait>(
        &'life0 mut self,
        source_id: Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn create_sink<'life0, 'async_trait>(
        &'life0 mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn search_sinks<'life0, 'async_trait>(
        &'life0 self,
        pagination: Pagination,
        query: QueryParams,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = SearchSourcesOrSinksResp>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn update_sink<'life0, 'async_trait>(
        &'life0 mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn delete_sink<'life0, 'async_trait>(
        &'life0 mut self,
        sink_id: Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn add_source_ref<'life0, 'life2, 'life3, 'async_trait>(
        &'life0 mut self,
        source_id: &'life2 Uuid,
        rule_id: &'life3 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn get_source_rx<'life0, 'life2, 'life3, 'async_trait>(
        &'life0 mut self,
        source_id: &'life2 Uuid,
        rule_id: &'life3 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<broadcast::Receiver<MessageBatch>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn del_source_rx<'life0, 'life2, 'life3, 'async_trait>(
        &'life0 mut self,
        source_id: &'life2 Uuid,
        rule_id: &'life3 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn del_source_ref<'life0, 'life2, 'life3, 'async_trait>(
        &'life0 mut self,
        source_id: &'life2 Uuid,
        rule_id: &'life3 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn add_sink_ref<'life0, 'life2, 'life3, 'async_trait>(
        &'life0 mut self,
        sink_id: &'life2 Uuid,
        rule_id: &'life3 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn get_sink_tx<'life0, 'life2, 'life3, 'async_trait>(
        &'life0 mut self,
        sink_id: &'life2 Uuid,
        rule_id: &'life3 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<mpsc::Sender<MessageBatch>>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn del_sink_tx<'life0, 'life2, 'life3, 'async_trait>(
        &'life0 mut self,
        sink_id: &'life2 Uuid,
        rule_id: &'life3 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn del_sink_ref<'life0, 'life2, 'life3, 'async_trait>(
        &'life0 mut self,
        sink_id: &'life2 Uuid,
        rule_id: &'life3 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    async fn start(&mut self) -> HaliaResult<()> {
        todo!()
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        todo!()
    }

    async fn write_source_value(
        &mut self,
        source_id: Uuid,
        req: Value,
    ) -> HaliaResult<()> {
        todo!()
    }
}
