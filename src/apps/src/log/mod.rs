use async_trait::async_trait;
use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
};
use message::MessageBatch;
use sink::Sink;
use tokio::sync::{broadcast, mpsc};
use types::{
    apps::{
        log::LogConf, AppConf, AppType, CreateUpdateAppReq, QueryParams, SearchAppsItemConf,
        SearchAppsItemResp,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp,
};
use uuid::Uuid;

use crate::{sink_not_found_err, App};

mod sink;

macro_rules! log_not_support_source {
    () => {
        Err(HaliaError::Common("日志应用不支持源".to_owned()))
    };
}

pub struct Log {
    pub id: Uuid,
    base_conf: BaseConf,
    ext_conf: LogConf,
    err: Option<String>,

    on: bool,
    sinks: Vec<Sink>,
}

pub fn new(app_id: Uuid, app_conf: AppConf) -> HaliaResult<Box<dyn App>> {
    let ext_conf: LogConf = serde_json::from_value(app_conf.ext)?;
    Log::validate_conf(&ext_conf)?;

    Ok(Box::new(Log {
        id: app_id,
        base_conf: app_conf.base,
        ext_conf,
        on: false,
        // err: Arc::new(RwLock::new(None)),
        err: None,
        sinks: vec![],
    }))
}

impl Log {
    fn validate_conf(conf: &LogConf) -> HaliaResult<()> {
        Ok(())
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
            id: self.id,
            app_type: AppType::Log,
            on: self.on,
            err: self.err.clone(),
            rtt: 0,
            conf: SearchAppsItemConf {
                base: self.base_conf.clone(),
                ext: serde_json::to_value(self.ext_conf.clone()).unwrap(),
            },
        }
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn update<'life0, 'async_trait>(
        &'life0 mut self,
        app_conf: AppConf,
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

    async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);
        for sink in self.sinks.iter_mut() {
            sink.start();
        }

        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        check_and_set_on_false!(self);

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
        let mut sink = Sink::new(sink_id, req)?;
        if self.on {
            sink.start();
        }
        self.sinks.push(sink);

        Ok(())
    }

    async fn search_sinks(
        &self,
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        todo!()
        // let mut total = 0;
        // let mut data = vec![];
        // for sink in self.sinks.iter().rev() {
        //     let sink = sink.search();

        //     if let Some(name) = &query.name {
        //         if !sink.conf.base.name.contains(name) {
        //             continue;
        //         }
        //     }

        //     if pagination.check(total) {
        //         data.push(sink);
        //     }

        //     total += 1;
        // }

        // SearchSourcesOrSinksResp { total, data }
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
            Some(sink) => {
                if !self.on {
                    return Err(HaliaError::Stopped(format!(
                        "应用日志: {}",
                        self.base_conf.name.clone()
                    )));
                }
                Ok(sink.get_tx(rule_id))
            }
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
