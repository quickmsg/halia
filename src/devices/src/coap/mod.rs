use api::API;
use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    get_id,
    persistence::{self, Status},
};
use message::MessageBatch;
use sink::Sink;
use std::str::FromStr;
use tokio::sync::broadcast;
use types::{
    devices::{
        coap::{
            CreateUpdateAPIReq, CreateUpdateCoapReq, CreateUpdateSinkReq, SearchAPIsResp,
            SearchSinksResp,
        },
        SearchDevicesItemConf, SearchDevicesItemResp,
    },
    Pagination,
};
use uuid::Uuid;

pub const TYPE: &str = "coap";
mod api;
pub mod manager;
mod sink;

fn api_not_find_err(api_id: Uuid) -> HaliaError {
    HaliaError::NotFound("api".to_owned(), api_id)
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
            persistence::devices::coap::create(
                &device_id,
                TYPE,
                serde_json::to_string(&req).unwrap(),
            )
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

    pub fn search(&self) -> SearchDevicesItemResp {
        SearchDevicesItemResp {
            id: self.id.clone(),
            typ: TYPE,
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
            // restart
            todo!()
        }

        Ok(())
    }

    pub async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;

        for api in self.apis.iter_mut() {
            api.start(&self.conf.ext).await;
        }

        for sink in self.sinks.iter_mut() {
            sink.start(&self.conf.ext).await;
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if self.apis.iter().any(|api| !api.can_stop()) {
            return Err(HaliaError::Common("有api正被引用中".to_owned()));
        }
        if self.sinks.iter().any(|sink| sink.can_stop()) {
            return Err(HaliaError::Common("有动作正被引用中".to_owned()));
        }

        check_and_set_on_false!(self);

        for api in self.apis.iter_mut() {
            api.stop().await;
        }

        for sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if self.on {
            return Err(HaliaError::Running);
        }

        for api in self.apis.iter() {
            if !api.can_delete() {
                return Err(HaliaError::Common("有api正被引用中".to_owned()));
            }
        }

        for sink in self.sinks.iter() {
            if !sink.can_delete() {
                return Err(HaliaError::Common("有动作正被引用中".to_owned()));
            }
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
            None => Err(api_not_find_err(api_id)),
        }
    }

    pub async fn delete_api(&mut self, api_id: Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == api_id) {
            Some(api) => api.delete(&self.id).await?,
            None => return Err(api_not_find_err(api_id)),
        }
        self.apis.retain(|api| api.id != api_id);
        Ok(())
    }

    pub async fn add_api_ref(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.add_ref(rule_id)),
            None => Err(api_not_find_err(api_id.clone())),
        }
    }

    pub async fn get_api_mb_rx(
        &mut self,
        api_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.get_mb_rx(rule_id)),
            None => Err(api_not_find_err(api_id.clone())),
        }
    }

    pub async fn del_api_ref(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.del_ref(rule_id)),
            None => Err(api_not_find_err(api_id.clone())),
        }
    }

    pub async fn del_api_mb_rx(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.del_mb_rx(rule_id)),
            None => Err(api_not_find_err(api_id.clone())),
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
            // TODO
            None => Err(api_not_find_err(sink_id)),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id != sink_id);
                Ok(())
            }
            None => todo!(),
        }
    }
}
