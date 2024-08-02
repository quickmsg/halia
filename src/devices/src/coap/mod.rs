use api::API;
use common::{
    error::{HaliaError, HaliaResult},
    persistence::{self, Status},
};
use message::MessageBatch;
use protocol::coap::client::UdpCoAPClient;
use sink::Sink;
use std::{str::FromStr, sync::Arc};
use tokio::sync::{broadcast, mpsc};
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

pub struct Coap {
    id: Uuid,
    conf: CreateUpdateCoapReq,
    client: Option<Arc<UdpCoAPClient>>,
    apis: Vec<API>,
    sinks: Vec<Sink>,

    on: bool,
    err: Option<String>,
}

impl Coap {
    pub async fn new(device_id: Option<Uuid>, req: CreateUpdateCoapReq) -> HaliaResult<Self> {
        let (device_id, new) = match device_id {
            Some(device_id) => (device_id, false),
            None => (Uuid::new_v4(), true),
        };

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
            client: None,
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
        // TODO
        SearchDevicesItemResp {
            id: self.id.clone(),
            typ: TYPE,
            on: self.client.is_some(),
            err: self.err.clone(),
            rtt: 999,
            conf: SearchDevicesItemConf {
                base: todo!(),
                ext: todo!(),
            },
        }
    }

    pub async fn update(&mut self, req: CreateUpdateCoapReq) -> HaliaResult<()> {
        persistence::devices::update_device_conf(&self.id, serde_json::to_string(&req).unwrap())
            .await?;

        let mut restart = false;
        if self.conf.host != req.host || self.conf.port != req.port {
            restart = true;
        }

        self.conf = req;

        if restart && self.client.is_some() {
            // restart
            todo!()
        }

        Ok(())
    }

    pub async fn start(&mut self) -> HaliaResult<()> {
        if self.on {
            return Ok(());
        } else {
            self.on = true;
        }

        persistence::devices::update_device_status(&self.id, Status::Runing).await?;

        let client = UdpCoAPClient::new_udp((self.conf.host.clone(), self.conf.port)).await?;
        let clone_client = Arc::new(client);
        for api in self.apis.iter_mut() {
            api.start(clone_client.clone()).await;
        }
        self.client = Some(clone_client);

        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        if !self.on {
            return Ok(());
        } else {
            self.on = false;
        }
        persistence::devices::update_device_status(&self.id, Status::Stopped).await?;
        for api in self.apis.iter_mut() {
            api.stop().await;
        }
        self.client = None;

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if self.client.is_some() {
            return Err(HaliaError::DeviceRunning);
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
            api.start(self.client.as_ref().unwrap().clone()).await;
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
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn delete_api(&mut self, api_id: Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == api_id) {
            Some(api) => api.delete(&self.id).await?,
            None => return Err(HaliaError::NotFound),
        }
        self.apis.retain(|api| api.id != api_id);
        Ok(())
    }

    pub async fn add_api_ref(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.add_ref(rule_id)),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn subscribe(
        &mut self,
        api_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.subscribe(rule_id)),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn unsubscribe(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.unsubscribe(rule_id)),
            None => Err(HaliaError::NotFound),
        }
    }

    pub async fn del_api_ref(&mut self, api_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.apis.iter_mut().find(|api| api.id == *api_id) {
            Some(api) => Ok(api.del_ref(rule_id)),
            None => Err(HaliaError::NotFound),
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
            None => Err(HaliaError::NotFound),
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

    pub async fn publish(&self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        todo!()
    }
}
