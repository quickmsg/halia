use async_trait::async_trait;
use common::{
    active_sink_ref, active_source_ref, add_sink_ref, add_source_ref, check_and_set_on_true,
    check_delete, check_delete_sink, check_delete_source, check_stop, deactive_sink_ref,
    deactive_source_ref, del_sink_ref, del_source_ref,
    error::{HaliaError, HaliaResult},
    ref_info::RefInfo,
};
use message::MessageBatch;
use sink::Sink;
use source::Source;
use tokio::sync::{broadcast, mpsc};
use types::{
    apps::{
        http_client::{HttpClientConf, SinkConf, SourceConf},
        AppConf, AppType, CreateUpdateAppReq, QueryParams, SearchAppsItemCommon,
        SearchAppsItemConf, SearchAppsItemResp,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksItemResp,
    SearchSourcesOrSinksResp,
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
    sources_ref_infos: Vec<(Uuid, RefInfo)>,
    sinks: Vec<Sink>,
    sinks_ref_infos: Vec<(Uuid, RefInfo)>,
}

pub fn new(app_id: Uuid, app_conf: AppConf) -> HaliaResult<Box<dyn App>> {
    let ext_conf: HttpClientConf = serde_json::from_value(app_conf.ext)?;
    HttpClient::validate_conf(&ext_conf)?;

    Ok(Box::new(HttpClient {
        id: app_id,
        base_conf: app_conf.base,
        ext_conf,
        on: false,
        err: None,
        sources: vec![],
        sources_ref_infos: vec![],
        sinks: vec![],
        sinks_ref_infos: vec![],
    }))
}

impl HttpClient {
    fn validate_conf(_conf: &HttpClientConf) -> HaliaResult<()> {
        Ok(())
    }

    fn check_on(&self) -> HaliaResult<()> {
        match self.on {
            true => Ok(()),
            false => Err(HaliaError::Stopped(format!(
                "http客户端应用:{}",
                self.base_conf.name
            ))),
        }
    }
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
            common: SearchAppsItemCommon {
                id: self.id.clone(),
                app_type: AppType::HttpClient,
                on: false,
                err: None,
                rtt: 999,
            },
            conf: SearchAppsItemConf {
                base: self.base_conf.clone(),
                ext: serde_json::to_value(&self.ext_conf).unwrap(),
            },
        }
    }

    async fn update(&mut self, app_conf: AppConf) -> HaliaResult<()> {
        let ext_conf: HttpClientConf = serde_json::from_value(app_conf.ext)?;
        HttpClient::validate_conf(&ext_conf)?;

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
        check_stop!(self, sources_ref_infos);
        check_stop!(self, sinks_ref_infos);

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
        check_delete!(self, sources_ref_infos);
        check_delete!(self, sinks_ref_infos);

        if self.on {
            for source in self.sources.iter_mut() {
                source.stop().await;
            }
            for sink in self.sinks.iter_mut() {
                sink.stop().await;
            }
        }

        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;

        for source in self.sources.iter() {
            source.check_duplicate(&req.base, &ext_conf)?;
        }

        let mut source = Source::new(source_id, req.base, ext_conf)?;

        if self.on {
            source.start(self.ext_conf.clone()).await;
        }

        self.sources.push(source);
        self.sources_ref_infos.push((source_id, RefInfo::new()));

        Ok(())
    }

    async fn search_sources(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut total = 0;
        let mut data = vec![];
        for (index, source) in self.sources.iter().rev().enumerate() {
            let source = source.search();
            if let Some(name) = &query.name {
                if !source.conf.base.name.contains(name) {
                    continue;
                }
            }

            if pagination.check(total) {
                unsafe {
                    data.push(SearchSourcesOrSinksItemResp {
                        info: source,
                        rule_ref: self.sources_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    })
                }
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
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;

        for source in self.sources.iter() {
            if source.id != source_id {
                source.check_duplicate(&req.base, &ext_conf)?;
            }
        }

        match self
            .sources
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => source.update(req.base, ext_conf).await,
            None => source_not_found_err!(),
        }
    }

    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()> {
        check_delete_source!(self, source_id);

        if self.on {
            match self
                .sources
                .iter_mut()
                .find(|source| source.id == source_id)
            {
                Some(source) => source.stop().await,
                None => unreachable!(),
            }
        }

        self.sources.retain(|source| source.id != source_id);
        self.sources_ref_infos.retain(|(id, _)| *id != source_id);

        Ok(())
    }

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        for sink in self.sinks.iter() {
            sink.check_duplicate(&req.base, &ext_conf)?;
        }

        let mut sink = Sink::new(sink_id, req.base, ext_conf)?;
        if self.on {
            sink.start(self.ext_conf.clone()).await;
        }

        self.sinks.push(sink);
        self.sinks_ref_infos.push((sink_id, RefInfo::new()));

        Ok(())
    }

    async fn search_sinks(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut data = vec![];
        let mut total = 0;
        for (index, sink) in self.sinks.iter().rev().enumerate() {
            let sink = sink.search();
            if let Some(name) = &query.name {
                if !sink.conf.base.name.contains(name) {
                    continue;
                }
            }

            if total >= (pagination.page - 1) * pagination.size
                && total < pagination.page * pagination.size
            {
                unsafe {
                    data.push(SearchSourcesOrSinksItemResp {
                        info: sink,
                        rule_ref: self.sinks_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    })
                }
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
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        for sink in self.sinks.iter() {
            if sink.id != sink_id {
                sink.check_duplicate(&req.base, &ext_conf)?;
            }
        }

        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => match sink.update(req.base, ext_conf).await {
                Ok(()) => Ok(()),
                Err(e) => Err(e),
            },
            None => sink_not_found_err!(),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        check_delete_sink!(self, sink_id);
        if self.on {
            match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
                Some(sink) => sink.stop().await,
                None => unreachable!(),
            }
        }

        self.sinks.retain(|sink| sink.id != sink_id);
        self.sinks_ref_infos.retain(|(id, _)| *id != sink_id);
        Ok(())
    }

    async fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        add_source_ref!(self, source_id, rule_id)
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        self.check_on()?;
        active_source_ref!(self, source_id, rule_id);

        match self
            .sources
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.mb_tx.as_ref().unwrap().subscribe()),
            None => unreachable!(),
        }
    }

    async fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        deactive_source_ref!(self, source_id, rule_id)
    }

    async fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_source_ref!(self, source_id, rule_id)
    }

    async fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        add_sink_ref!(self, sink_id, rule_id)
    }

    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        self.check_on()?;
        active_sink_ref!(self, sink_id, rule_id);
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.mb_tx.as_ref().unwrap().clone()),
            None => unreachable!(),
        }
    }

    async fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        deactive_sink_ref!(self, sink_id, rule_id)
    }

    async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_sink_ref!(self, sink_id, rule_id)
    }
}
