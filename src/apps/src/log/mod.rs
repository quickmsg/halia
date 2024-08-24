use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
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

use crate::App;

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
    stop_signal_tx: Option<mpsc::Sender<()>>,
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
        stop_signal_tx: None,
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
        todo!()
    }

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn stop<'life0, 'async_trait>(
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
        pagination: Pagination,
        query: QueryParams,
    ) -> SearchSourcesOrSinksResp {
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

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
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

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn get_source_rx<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 mut self,
        source_id: &'life1 Uuid,
        rule_id: &'life2 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<broadcast::Receiver<MessageBatch>>>
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

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn del_source_rx<'life0, 'life1, 'life2, 'async_trait>(
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

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn del_source_ref<'life0, 'life1, 'life2, 'async_trait>(
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

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn add_sink_ref<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 mut self,
        sink_id: &'life1 Uuid,
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

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn get_sink_tx<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 mut self,
        sink_id: &'life1 Uuid,
        rule_id: &'life2 Uuid,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = HaliaResult<mpsc::Sender<MessageBatch>>>
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

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn del_sink_tx<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 mut self,
        sink_id: &'life1 Uuid,
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

    #[must_use]
    #[allow(clippy::type_complexity, clippy::type_repetition_in_bounds)]
    fn del_sink_ref<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 mut self,
        sink_id: &'life1 Uuid,
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
}
