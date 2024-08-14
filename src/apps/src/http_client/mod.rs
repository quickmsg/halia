use std::str::FromStr;

use common::{
    check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    get_id, persistence,
};
use sink::Sink;
use source::Source;
use types::{
    apps::{
        http_client::{
            CreateUpdateHttpClientReq, CreateUpdateSinkReq, CreateUpdateSourceReq, SearchSinksResp,
            SearchSourcesResp,
        },
        SearchAppsItemConf, SearchAppsItemResp,
    },
    Pagination,
};
use uuid::Uuid;

pub const TYPE: &str = "http_client";

pub mod manager;
mod sink;
mod source;

macro_rules! source_not_found_err {
    ($source_id:expr) => {
        Err(HaliaError::NotFound("http客户端源".to_owned(), $source_id))
    };
}

macro_rules! sink_not_found_err {
    ($sink_id:expr) => {
        Err(HaliaError::NotFound("http客户端动作".to_owned(), $sink_id))
    };
}

pub struct HttpClient {
    pub id: Uuid,

    on: bool,
    err: Option<String>,
    conf: CreateUpdateHttpClientReq,

    sources: Vec<Source>,
    sinks: Vec<Sink>,
}

impl HttpClient {
    pub async fn new(app_id: Option<Uuid>, req: CreateUpdateHttpClientReq) -> HaliaResult<Self> {
        HttpClient::check_conf(&req)?;

        let (app_id, new) = get_id(app_id);
        if new {
            persistence::apps::mqtt_client::create(
                &app_id,
                TYPE,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: app_id,
            conf: req,
            on: false,
            err: None,
            sources: vec![],
            sinks: vec![],
        })
    }

    fn check_conf(req: &CreateUpdateHttpClientReq) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateHttpClientReq) -> HaliaResult<()> {
        if self.conf.base.name == req.base.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub async fn recover(&mut self) -> HaliaResult<()> {
        match persistence::apps::http_client::read_sinks(&self.id).await {
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

    pub async fn update(&mut self, req: CreateUpdateHttpClientReq) -> HaliaResult<()> {
        HttpClient::check_conf(&req)?;

        persistence::apps::update_app_conf(&self.id, serde_json::to_string(&req).unwrap()).await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;

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

    pub async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);
        for source in self.sources.iter_mut() {
            source.start(self.conf.ext.clone()).await;
        }

        for sink in self.sinks.iter_mut() {
            sink.start(self.conf.ext.clone()).await;
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
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

    pub async fn delete(&mut self) -> HaliaResult<()> {
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

        persistence::apps::http_client::delete(&self.id).await?;
        Ok(())
    }

    fn search(&self) -> SearchAppsItemResp {
        SearchAppsItemResp {
            id: self.id,
            on: self.on,
            typ: TYPE,
            conf: SearchAppsItemConf {
                base: self.conf.base.clone(),
                ext: serde_json::to_value(&self.conf.ext).unwrap(),
            },
            err: self.err.clone(),
        }
    }

    async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match sink::new(&self.id, sink_id, req).await {
            Ok(sink) => {
                self.sinks.push(sink);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sinks(&self, pagination: Pagination) -> SearchSinksResp {
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
            Some(sink) => match sink.update(&self.id, req).await {
                Ok(()) => Ok(()),
                Err(e) => Err(e),
            },
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

    async fn create_source(
        &mut self,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceReq,
    ) -> HaliaResult<()> {
        for source in self.sources.iter() {
            source.check_duplicate(&req)?;
        }

        match Source::new(&self.id, source_id, req).await {
            Ok(source) => {
                self.sources.push(source);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sources(&self, pagination: Pagination) -> SearchSourcesResp {
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
        SearchSourcesResp {
            total: self.sinks.len(),
            data,
        }
    }

    pub async fn update_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceReq,
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
            None => source_not_found_err!(source_id),
        }
    }

    pub async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()> {
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
            None => source_not_found_err!(source_id),
        }
    }
}
