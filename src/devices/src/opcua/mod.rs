use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common::{
    check_and_set_on_false, check_and_set_on_true, check_delete, check_delete_all,
    error::{HaliaError, HaliaResult},
    ref_info::RefInfo,
};
use message::MessageBatch;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    types::{EndpointDescription, StatusCode},
};
use paste::paste;
use sink::Sink;
use source::Source;
use tokio::{
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
};
use tracing::debug;
use types::{
    devices::{
        opcua::{CreateUpdateSinkReq, OpcuaConf, SearchSinksResp, SourceConf},
        CreateUpdateDeviceReq, DeviceConf, DeviceType, QueryParams, SearchDevicesItemCommon,
        SearchDevicesItemConf, SearchDevicesItemResp,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksInfoResp,
    SearchSourcesOrSinksItemResp, SearchSourcesOrSinksResp, Value,
};
use uuid::Uuid;

use crate::Device;

mod group;
mod monitored_item;
mod sink;
mod source;
mod subscription;
mod variable;

struct Opcua {
    id: Uuid,
    base_conf: BaseConf,
    ext_conf: OpcuaConf,

    on: bool,
    err: Option<String>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    session: Arc<RwLock<Option<Arc<Session>>>>,

    sources: Vec<Source>,
    source_ref_infos: Vec<(Uuid, RefInfo)>,
    sinks: Vec<Sink>,
    sink_ref_infos: Vec<(Uuid, RefInfo)>,
}

pub async fn new(id: Uuid, device_conf: DeviceConf) -> HaliaResult<Box<dyn Device>> {
    let ext_conf: OpcuaConf = serde_json::from_value(device_conf.ext)?;
    Opcua::validate_conf(&ext_conf)?;

    Ok(Box::new(Opcua {
        id: id,
        base_conf: device_conf.base,
        ext_conf,
        on: false,
        err: None,
        session: Arc::new(RwLock::new(None)),
        stop_signal_tx: None,
        sources: vec![],
        source_ref_infos: vec![],
        sinks: vec![],
        sink_ref_infos: vec![],
    }))
}

impl Opcua {
    fn validate_conf(_conf: &OpcuaConf) -> HaliaResult<()> {
        Ok(())
    }

    async fn connect(
        opcua_conf: &OpcuaConf,
    ) -> HaliaResult<(Arc<Session>, JoinHandle<StatusCode>)> {
        let mut client = ClientBuilder::new()
            .application_name("test")
            .application_uri("aasda")
            .trust_server_certs(true)
            .session_retry_limit(3)
            .create_sample_keypair(true)
            .keep_alive_interval(Duration::from_millis(100))
            .client()
            .unwrap();

        let endpoint: EndpointDescription = EndpointDescription::from(opcua_conf.host.as_ref());

        let (session, event_loop) = match client
            .new_session_from_endpoint(endpoint, IdentityToken::Anonymous)
            .await
        {
            Ok((session, event_loop)) => (session, event_loop),
            Err(e) => {
                debug!("{:?}", e);
                return Err(HaliaError::Common(e.to_string()));
            }
        };

        let handle = event_loop.spawn();
        session.wait_for_connection().await;
        Ok((session, handle))
    }

    fn search(&self) -> SearchDevicesItemResp {
        todo!()
        // SearchDevicesItemResp {
        //     id: self.id,
        //     device_type: DeviceType::Opcua,
        //     on: self.on,
        //     err: self.err.clone(),
        //     rtt: 9999,
        //     conf: SearchDevicesItemConf {
        //         base: self.conf.base.clone(),
        //         ext: serde_json::json!(self.conf.ext),
        //     },
        // }
    }

    // async fn start(&mut self) -> HaliaResult<()> {
    //     check_and_set_on_true!(self);

    //     persistence::update_device_status(&self.id, Status::Runing).await?;

    //     let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
    //     self.stop_signal_tx = Some(stop_signal_tx);

    //     self.event_loop(stop_signal_rx).await;
    //     Ok(())
    // }

    // async fn event_loop(&mut self, mut stop_signal_rx: mpsc::Receiver<()>) {
    //     let opcua_conf = self.conf.ext.clone();
    //     let global_session = self.session.clone();
    //     let reconnect = self.conf.ext.reconnect;
    //     let groups = self.groups.clone();
    //     tokio::spawn(async move {
    //         loop {
    //             match Opcua::connect(&opcua_conf).await {
    //                 Ok((session, join_handle)) => {
    //                     for group in groups.write().await.iter_mut() {
    //                         group.start(session.clone()).await;
    //                     }

    //                     *(global_session.write().await) = Some(session);
    //                     match join_handle.await {
    //                         Ok(s) => {
    //                             debug!("{}", s);
    //                         }
    //                         Err(e) => debug!("{}", e),
    //                     }
    //                 }
    //                 Err(e) => {
    //                     let sleep = time::sleep(Duration::from_secs(reconnect));
    //                     tokio::pin!(sleep);
    //                     select! {
    //                         _ = stop_signal_rx.recv() => {
    //                             return
    //                         }

    //                         _ = &mut sleep => {}
    //                     }
    //                     debug!("{e}");
    //                 }
    //             }
    //         }
    //     });
    // }

    async fn stop(&mut self) -> HaliaResult<()> {
        check_and_set_on_false!(self);

        // for group in self.groups.write().await.iter_mut() {
        //     group.stop().await?;
        // }

        match self
            .session
            .read()
            .await
            .as_ref()
            .unwrap()
            .disconnect()
            .await
        {
            Ok(_) => {
                debug!("session disconnect success");
            }
            Err(e) => {
                debug!("err code is :{}", e);
            }
        }

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        // if self
        //     .groups
        //     .read()
        //     .await
        //     .iter()
        //     .any(|group| !group.ref_info.can_delete())
        // {
        //     return Err(HaliaError::Common("有组被规则引用中！".to_owned()));
        // }

        // if self
        //     .subscriptions
        //     .iter()
        //     .any(|subscription| !subscription.ref_info.can_delete())
        // {
        //     return Err(HaliaError::Common("有订阅被规则引用中！".to_owned()));
        // }

        todo!()
    }

    async fn create_sink(&mut self, sink_id: Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        match Sink::new(sink_id, req).await {
            Ok(sink) => {
                if self.on {
                    //
                }
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

    async fn update_sink(&mut self, sink_id: Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => sink.update(&self.id, req).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id != sink_id);
                Ok(())
            }
            None => Err(HaliaError::NotFound),
        }
    }
}

#[async_trait]
impl Device for Opcua {
    fn get_id(&self) -> &Uuid {
        &self.id
    }

    fn check_duplicate(&self, req: &CreateUpdateDeviceReq) -> HaliaResult<()> {
        if self.base_conf.name == req.conf.base.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    async fn read(&self) -> SearchDevicesItemResp {
        // let err = self.err.read().await.clone();
        let rtt = match (self.on, &self.err) {
            (true, None) => Some(999),
            _ => None,
        };
        SearchDevicesItemResp {
            common: SearchDevicesItemCommon {
                id: self.id.clone(),
                device_type: DeviceType::Opcua,
                on: self.on,
                err: self.err.clone(),
                rtt,
            },
            conf: SearchDevicesItemConf {
                base: self.base_conf.clone(),
                ext: serde_json::json!(self.ext_conf),
            },
            source_cnt: self.source_ref_infos.len(),
            sink_cnt: self.sink_ref_infos.len(),
        }
    }

    async fn update(&mut self, device_conf: DeviceConf) -> HaliaResult<()> {
        let ext_conf: OpcuaConf = serde_json::from_value(device_conf.ext)?;
        Self::validate_conf(&ext_conf)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = device_conf.base;
        self.ext_conf = ext_conf;

        if restart && self.on {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();
        }

        Ok(())
    }

    async fn delete(&mut self) -> HaliaResult<()> {
        check_delete_all!(self, source);
        check_delete_all!(self, sink);

        if self.on {
            self.stop().await?;
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
        if self.on {}

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
                        rule_ref: self.source_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    });
                }
            }

            total += 1;
        }

        SearchSourcesOrSinksResp { total, data }
    }

    async fn read_source(&self, source_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp> {
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

    async fn read_sink(&self, sink_id: &Uuid) -> HaliaResult<SearchSourcesOrSinksInfoResp> {
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

    fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        todo!()
    }

    fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    fn add_sink_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        todo!()
    }

    fn del_sink_tx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    fn del_sink_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        todo!()
    }

    async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        // self.event_loop(stop_signal_rx).await;
        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        todo!()
    }
    async fn write_source_value(&mut self, source_id: Uuid, req: Value) -> HaliaResult<()> {
        todo!()
    }
}
