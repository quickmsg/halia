use async_trait::async_trait;
use common::{
    check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    persistence,
};
use message::MessageBatch;
use sink::Sink;
use source::Source;
use tokio::sync::{broadcast, mpsc};
use types::{
    apps::{
        http_client::HttpClientConf, AppConf, AppType, CreateUpdateAppReq, QueryParams,
        SearchAppsItemConf, SearchAppsItemResp,
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

pub fn new(app_id: Uuid, app_conf: AppConf) -> HaliaResult<Box<dyn App>> {
    let ext_conf: HttpClientConf = serde_json::from_value(app_conf.ext)?;

    Ok(Box::new(HttpClient {
        id: app_id,
        base_conf: app_conf.base,
        ext_conf,
        on: false,
        err: None,
        sources: vec![],
        sinks: vec![],
    }))
}

#[async_trait]
impl App for HttpClient {
    fn get_id(&self) -> &Uuid {
        &self.id
    }

    fn check_duplicate(&self, req: &CreateUpdateAppReq) -> HaliaResult<()> {
        if self.base_conf.name == req.conf.base.name {
            return Err(HaliaError::NameExists);
        }

        if req.app_type == AppType::HttpClient {
            // TODO
        }

        Ok(())
    }

    async fn search(&self) -> SearchAppsItemResp {
        SearchAppsItemResp {
            id: self.id,
            on: self.on,
            app_type: AppType::HttpClient,
            conf: SearchAppsItemConf {
                base: self.base_conf.clone(),
                ext: serde_json::to_value(&self.ext_conf).unwrap(),
            },
            err: self.err.clone(),
            rtt: 1,
        }
    }

    async fn update(&mut self, app_conf: AppConf) -> HaliaResult<()> {
        let ext_conf: HttpClientConf = serde_json::from_value(app_conf.ext)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = app_conf.base;
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
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        // for source in self.sources.iter() {
        //     source.check_duplicate(&req)?;
        // }

        match Source::new(source_id, req).await {
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
        let mut total = 0;
        for source in self.sources.iter().rev() {
            let source =  source.search();
            if let Some(name) = &query.name {
                if !source.conf.base.name.contains(name) {
                    continue;
                }
            }

            if pagination.check(total) {
                data.push(source);
            }

            total += 1;
        }

        SearchSourcesOrSinksResp { total, data }
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
            Some(source) => source.update(req).await,
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
                source.delete().await?;
                self.sources.retain(|source| source.id != source_id);
                Ok(())
            }
            None => source_not_found_err!(),
        }
    }

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(sink_id, req).await {
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
        let mut total = 0;
        for sink in self.sinks.iter().rev() {
            let sink = sink.search();
            if let Some(name) = &query.name {
                if !sink.conf.base.name.contains(name) {
                    continue;
                }
            }

            if total >= (pagination.page - 1) * pagination.size
                && total < pagination.page * pagination.size
            {
                data.push(sink);
            }

            total += 1;
        }
        SearchSourcesOrSinksResp { total, data }
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

    async fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .sources
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.ref_info.add_ref(rule_id)),
            None => source_not_found_err!(),
        }
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        if !self.on {
            return Err(HaliaError::Stopped(format!(
                "应用http客户端:{}",
                self.base_conf.name
            )));
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
            return Err(HaliaError::Stopped(format!(
                "应用http客户端: {}",
                self.base_conf.name
            )));
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
