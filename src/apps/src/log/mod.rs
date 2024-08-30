use async_trait::async_trait;
use common::{
    active_ref, add_ref, check_and_set_on_false, check_and_set_on_true, check_delete,
    check_delete_all, deactive_ref, del_ref,
    error::{HaliaError, HaliaResult},
    ref_info::RefInfo,
};
use message::MessageBatch;
use paste::paste;
use sink::Sink;
use tokio::sync::{broadcast, mpsc};
use types::{
    apps::{
        log::{LogConf, SinkConf},
        AppConf, AppType, CreateUpdateAppReq, QueryParams, SearchAppsItemCommon,
        SearchAppsItemConf, SearchAppsItemResp,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksInfoResp,
    SearchSourcesOrSinksItemResp, SearchSourcesOrSinksResp,
};
use uuid::Uuid;

use crate::App;

mod sink;

macro_rules! log_not_support_source {
    () => {
        Err(HaliaError::Common("日志应用不支持源!".to_owned()))
    };
}

pub struct Log {
    pub id: Uuid,
    base_conf: BaseConf,
    ext_conf: LogConf,
    err: Option<String>,

    on: bool,
    sinks: Vec<Sink>,
    sink_ref_infos: Vec<(Uuid, RefInfo)>,
}

pub fn new(app_id: Uuid, app_conf: AppConf) -> HaliaResult<Box<dyn App>> {
    let ext_conf: LogConf = serde_json::from_value(app_conf.ext)?;
    Log::validate_conf(&ext_conf)?;

    Ok(Box::new(Log {
        id: app_id,
        base_conf: app_conf.base,
        ext_conf,
        on: false,
        err: None,
        sinks: vec![],
        sink_ref_infos: vec![],
    }))
}

impl Log {
    fn validate_conf(_conf: &LogConf) -> HaliaResult<()> {
        Ok(())
    }

    fn check_on(&self) -> HaliaResult<()> {
        match self.on {
            true => Ok(()),
            false => Err(HaliaError::Stopped(format!(
                "log应用:{}",
                self.base_conf.name
            ))),
        }
    }
}

#[async_trait]
impl App for Log {
    fn get_id(&self) -> &Uuid {
        &self.id
    }

    fn check_duplicate(&self, req: &CreateUpdateAppReq) -> HaliaResult<()> {
        if self.base_conf.name == req.conf.base.name {
            return Err(HaliaError::NameExists);
        }

        if req.app_type == AppType::Log {
            // TODO
        }

        Ok(())
    }

    async fn search(&self) -> SearchAppsItemResp {
        SearchAppsItemResp {
            common: SearchAppsItemCommon {
                id: self.id,
                app_type: AppType::Log,
                on: self.on,
                err: self.err.clone(),
                rtt: 0,
            },
            conf: SearchAppsItemConf {
                base: self.base_conf.clone(),
                ext: serde_json::to_value(self.ext_conf.clone()).unwrap(),
            },
        }
    }

    async fn update(&mut self, app_conf: AppConf) -> HaliaResult<()> {
        // let ext_conf = serde_json::from_value(app_conf.ext)?;
        self.base_conf = app_conf.base;

        Ok(())
    }

    async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);
        for sink in self.sinks.iter_mut() {
            sink.start();
        }

        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        check_and_set_on_false!(self);
        for sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        Ok(())
    }

    async fn delete(&mut self) -> HaliaResult<()> {
        check_delete_all!(self, sink);

        if self.on {
            for sink in self.sinks.iter_mut() {
                sink.stop().await;
            }
        }

        Ok(())
    }

    async fn create_source(
        &mut self,
        _source_id: Uuid,
        _req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        log_not_support_source!()
    }

    async fn search_sources(
        &self,
        _pagination: Pagination,
        _query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        SearchSourcesOrSinksResp {
            total: 0,
            data: vec![],
        }
    }

    async fn search_source(&self, _source_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp> {
        log_not_support_source!()
    }

    async fn update_source(
        &mut self,
        _source_id: Uuid,
        _req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        log_not_support_source!()
    }

    async fn delete_source(&mut self, _source_id: Uuid) -> HaliaResult<()> {
        log_not_support_source!()
    }

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        Sink::validate_conf(&ext_conf)?;

        for sink in self.sinks.iter() {
            sink.check_duplicate(&req.base, &ext_conf)?;
        }

        let mut sink = Sink::new(sink_id, req.base, ext_conf);
        if self.on {
            sink.start();
        }

        self.sinks.push(sink);
        self.sink_ref_infos.push((sink_id, RefInfo::new()));

        Ok(())
    }

    async fn search_sinks(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut total = 0;
        let mut data = vec![];
        for (index, sink) in self.sinks.iter().rev().enumerate() {
            let sink = sink.search();
            if let Some(name) = &query.name {
                if !sink.conf.base.name.contains(name) {
                    continue;
                }
            }

            if pagination.check(total) {
                unsafe {
                    data.push(SearchSourcesOrSinksItemResp {
                        info: sink,
                        rule_ref: self.sink_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    })
                }
            }

            total += 1;
        }
        SearchSourcesOrSinksResp { total, data }
    }

    async fn search_sink(&self, sink_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp> {
        match self.sinks.iter().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.search()),
            None => Err(HaliaError::NotFound),
        }
    }

    async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        Sink::validate_conf(&ext_conf)?;

        for sink in self.sinks.iter() {
            if sink.id != sink_id {
                sink.check_duplicate(&req.base, &ext_conf)?;
            }
        }

        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => Ok(sink.update(req.base, ext_conf).await),
            None => Err(HaliaError::NotFound),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        check_delete!(self, sink, sink_id);

        if self.on {
            match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
                Some(sink) => sink.stop().await,
                None => unreachable!(),
            }
        }

        self.sinks.retain(|sink| sink.id != sink_id);
        self.sink_ref_infos.retain(|(id, _)| *id != sink_id);

        Ok(())
    }

    async fn add_source_ref(&mut self, _source_id: &Uuid, _rule_id: &Uuid) -> HaliaResult<()> {
        log_not_support_source!()
    }

    async fn get_source_rx(
        &mut self,
        _source_id: &Uuid,
        _rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        log_not_support_source!()
    }

    async fn del_source_rx(&mut self, _source_id: &Uuid, _rule_id: &Uuid) -> HaliaResult<()> {
        log_not_support_source!()
    }

    async fn del_source_ref(&mut self, _source_id: &Uuid, _rule_id: &Uuid) -> HaliaResult<()> {
        log_not_support_source!()
    }

    async fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        add_ref!(self, sink, sink_id, rule_id)
    }

    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        self.check_on()?;
        active_ref!(self, sink, sink_id, rule_id);
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.mb_tx.as_ref().unwrap().clone()),
            None => unreachable!(),
        }
    }

    async fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        deactive_ref!(self, sink, sink_id, rule_id)
    }

    async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_ref!(self, sink, sink_id, rule_id)
    }
}
