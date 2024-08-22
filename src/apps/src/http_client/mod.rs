use std::str::FromStr;

use async_trait::async_trait;
use common::{
    check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    get_id, persistence,
};
use message::MessageBatch;
use sink::Sink;
use source::Source;
use tokio::sync::{broadcast, mpsc};
use types::{
    apps::{
        http_client::HttpClientConf, AppType, CreateUpdateAppReq, QueryParams, SearchAppsItemConf,
        SearchAppsItemResp,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp,
};
use uuid::Uuid;

use crate::{sink_not_found_err, source_not_found_err, App};

mod sink;
mod source;

pub struct HttpClient {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: HttpClientConf,

    on: bool,
    err: Option<String>,

    sources: Vec<Source>,
    sinks: Vec<Sink>,
}

pub async fn new(
    app_id: Option<Uuid>,
    req: CreateUpdateAppReq,
    // ca: Option<Bytes>,
    // client_cert: Option<Bytes>,
    // client_key: Option<Bytes>,
) -> HaliaResult<Box<dyn App>> {
    let (base_conf, ext_conf, data) = HttpClient::parse_conf(req)?;

    let (app_id, new) = get_id(app_id);
    if new {
        persistence::create_app(&app_id, &data).await?;
    }

    Ok(Box::new(HttpClient {
        id: app_id,
        base_conf,
        ext_conf,
        on: false,
        err: None,
        sources: vec![],
        sinks: vec![],
    }))
}

impl HttpClient {
    fn parse_conf(req: CreateUpdateAppReq) -> HaliaResult<(BaseConf, HttpClientConf, String)> {
        let data = serde_json::to_string(&req)?;

        let conf: HttpClientConf = serde_json::from_value(req.conf.ext)?;

        Ok((req.conf.base, conf, data))
    }

    // pub fn check_duplicate(&self, req: &CreateUpdateHttpClientReq) -> HaliaResult<()> {
    //     if self.conf.base.name == req.base.name {
    //         return Err(HaliaError::NameExists);
    //     }

    //     Ok(())
    // }

    pub async fn recover(&mut self) -> HaliaResult<()> {
        match persistence::read_sinks(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);
                    let sink_id = Uuid::from_str(items[0]).unwrap();
                    self.create_sink(Some(sink_id), serde_json::from_str(items[1]).unwrap())
                        .await?;
                }
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }
}

#[async_trait]
impl App for HttpClient {
    fn get_id(&self) -> Uuid {
        self.id.clone()
    }

    async fn search(&self) -> SearchAppsItemResp {
        SearchAppsItemResp {
            id: self.id,
            on: self.on,
            typ: AppType::HttpClient,
            conf: SearchAppsItemConf {
                base: self.base_conf.clone(),
                ext: serde_json::to_value(&self.ext_conf).unwrap(),
            },
            err: self.err.clone(),
            rtt: 1,
        }
    }

    async fn update(&mut self, req: CreateUpdateAppReq) -> HaliaResult<()> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;

        persistence::update_app_conf(&self.id, &data).await?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        if self.on && restart {
            for source in self.sources.iter_mut() {
                source.restart().await;
            }

            for sink in self.sinks.iter_mut() {
                sink.restart().await;
            }
        }

        Ok(())
    }

    async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);
        for source in self.sources.iter_mut() {
            source.start(self.ext_conf.clone()).await;
        }

        for sink in self.sinks.iter_mut() {
            sink.start(self.ext_conf.clone()).await;
        }

        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        if self
            .sources
            .iter()
            .any(|source| !source.ref_info.can_stop())
        {
            return Err(HaliaError::StopActiveRefing);
        }
        if self.sinks.iter().any(|sink| !sink.ref_info.can_stop()) {
            return Err(HaliaError::StopActiveRefing);
        }

        check_and_set_on_true!(self);

        for source in self.sources.iter_mut() {
            source.stop().await;
        }
        for sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        Ok(())
    }

    async fn delete(&mut self) -> HaliaResult<()> {
        if self.on {
            return Err(HaliaError::Running);
        }

        if self
            .sources
            .iter()
            .any(|source| !source.ref_info.can_delete())
        {
            return Err(HaliaError::DeleteRefing);
        }
        if self.sinks.iter().any(|sink| !sink.ref_info.can_delete()) {
            return Err(HaliaError::DeleteRefing);
        }

        for source in self.sources.iter_mut() {
            source.stop().await;
        }
        for sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        persistence::delete_app(&self.id).await?;
        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        // for source in self.sources.iter() {
        //     source.check_duplicate(&req)?;
        // }

        match Source::new(&self.id, source_id, req).await {
            Ok(source) => {
                self.sources.push(source);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sources(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut data = vec![];
        for source in self
            .sources
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(source.search());
            if data.len() == pagination.size {
                break;
            }
        }

        SearchSourcesOrSinksResp {
            total: todo!(),
            data,
        }
    }

    async fn update_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match self
            .sources
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => match source.update(&self.id, req).await {
                Ok(()) => Ok(()),
                Err(e) => Err(e),
            },
            None => source_not_found_err!(),
        }
    }

    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()> {
        match self
            .sources
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => {
                source.delete(&self.id).await?;
                self.sources.retain(|source| source.id != source_id);
                Ok(())
            }
            None => source_not_found_err!(),
        }
    }

    async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(&self.id, sink_id, req).await {
            Ok(sink) => {
                self.sinks.push(sink);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sinks(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
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
        SearchSourcesOrSinksResp {
            total: self.sinks.len(),
            data,
        }
    }

    async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => match sink.update(&self.id, req).await {
                Ok(()) => Ok(()),
                Err(e) => Err(e),
            },
            None => sink_not_found_err!(),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id != sink_id);
                Ok(())
            }
            None => sink_not_found_err!(),
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn add_source_ref<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 mut self,
        source_id: &'life1 Uuid,
        rule_id: &'life2 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        if !self.on {
            return Err(HaliaError::Stopped);
        }
        match self
            .sources
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.get_rx(rule_id)),
            None => source_not_found_err!(),
        }
    }

    async fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        if !self.on {
            return Err(HaliaError::Stopped);
        }
        match self
            .sources
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.del_rx(rule_id)),
            None => source_not_found_err!(),
        }
    }

    async fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .sources
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.ref_info.del_ref(rule_id)),
            None => source_not_found_err!(),
        }
    }

    async fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.add_ref(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.get_tx(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    async fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.deactive_ref(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.del_ref(rule_id)),
            None => sink_not_found_err!(),
        }
    }
}
