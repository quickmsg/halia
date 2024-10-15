use std::{
    sync::{atomic::AtomicU16, Arc},
    time::Duration,
};

use anyhow::{bail, Result};
use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    types::{EndpointDescription, Identifier, NodeId, StatusCode},
};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{broadcast, mpsc, watch, RwLock},
    task::JoinHandle,
    time,
};
use tracing::{debug, warn};
use types::{
    devices::{
        opcua::{Conf, SinkConf, SourceConf},
        SearchDevicesItemRunningInfo,
    },
    Value,
};

use crate::{add_device_running_count, sub_device_running_count, Device};

mod sink;
mod source;

struct Opcua {
    err: Arc<RwLock<Option<String>>>,
    rtt: Arc<AtomicU16>,
    stop_signal_tx: watch::Sender<()>,
    opcua_client: Arc<RwLock<Option<Arc<Session>>>>,

    sources: DashMap<String, Source>,
    sinks: DashMap<String, Sink>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
}

struct JoinHandleData {
    pub id: String,
    pub conf: Conf,
    pub opcua_client: Arc<RwLock<Option<Arc<Session>>>>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub err: Arc<RwLock<Option<String>>>,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn Device> {
    let conf: Conf = serde_json::from_value(conf).unwrap();
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());

    let opcua_client: Arc<RwLock<Option<Arc<Session>>>> = Arc::new(RwLock::new(None));

    let err = Arc::new(RwLock::new(None));
    let join_handle_data = JoinHandleData {
        id,
        conf,
        opcua_client: opcua_client.clone(),
        stop_signal_rx,
        err: err.clone(),
    };

    let join_handle = Opcua::event_loop(join_handle_data);

    Box::new(Opcua {
        err,
        opcua_client,
        stop_signal_tx,
        sources: DashMap::new(),
        sinks: DashMap::new(),
        join_handle: Some(join_handle),
        rtt: Arc::new(AtomicU16::new(0)),
    })
}

pub fn validate_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
    Ok(())
}

pub fn validate_source_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SourceConf = serde_json::from_value(conf.clone())?;
    Source::validate_conf(conf)?;
    Ok(())
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(conf)?;
    Ok(())
}

impl Opcua {
    async fn connect(conf: &Conf) -> Result<(Arc<Session>, JoinHandle<StatusCode>)> {
        debug!("{}", conf.addr);
        let mut client = ClientBuilder::new()
            .application_name("test")
            .application_uri("aasda")
            .trust_server_certs(true)
            .session_retry_limit(3)
            .create_sample_keypair(true)
            .keep_alive_interval(Duration::from_millis(100))
            .client()
            .unwrap();

        let endpoint: EndpointDescription = EndpointDescription::from(conf.addr.as_ref());

        let (session, event_loop) = match client
            .new_session_from_endpoint(endpoint, IdentityToken::Anonymous)
            .await
        {
            Ok((session, event_loop)) => (session, event_loop),
            Err(e) => bail!(e.to_string()),
        };

        let handle = event_loop.spawn();
        // session.wait_for_connection().await;
        debug!("opcua connect success");
        Ok((session, handle))
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            let mut task_err: Option<String> = Some("not connectd.".to_owned());
            loop {
                match Opcua::connect(&join_handle_data.conf).await {
                    Ok((session, join_handle)) => {
                        debug!("opcua connect success");
                        task_err = None;
                        add_device_running_count();
                        events::insert_connect_succeed(
                            types::events::ResourceType::Device,
                            &join_handle_data.id,
                        )
                        .await;
                        join_handle_data.opcua_client.write().await.replace(session);
                        match join_handle.await {
                            Ok(s) => {
                                debug!("{}", s);
                            }
                            Err(e) => debug!("{}", e),
                        }
                    }
                    Err(e) => {
                        if task_err.is_none() {
                            sub_device_running_count();
                        }
                        debug!("{}", e);

                        events::insert_connect_failed(
                            types::events::ResourceType::Device,
                            &join_handle_data.id,
                            e.to_string(),
                        )
                        .await;
                        warn!("connect error: {}", e);
                        let sleep =
                            time::sleep(Duration::from_secs(join_handle_data.conf.reconnect));
                        tokio::pin!(sleep);
                        select! {
                            _ = join_handle_data.stop_signal_rx.changed() => {
                                return join_handle_data;
                            }

                            _ = &mut sleep => {}
                        }
                        debug!("{e}");
                    }
                }
            }
        })
    }
}

#[async_trait]
impl Device for Opcua {
    async fn read_running_info(&self) -> SearchDevicesItemRunningInfo {
        SearchDevicesItemRunningInfo {
            err: self.err.read().await.clone(),
            rtt: self.rtt.load(std::sync::atomic::Ordering::Relaxed),
        }
    }

    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let new_conf: Conf = serde_json::from_value(new_conf)?;
        self.stop_signal_tx.send(()).unwrap();
        let mut join_handle_data = self.join_handle.take().unwrap().await.unwrap();
        join_handle_data.conf = new_conf;
        let join_handle = Self::event_loop(join_handle_data);
        self.join_handle = Some(join_handle);

        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        let source = Source::new(self.opcua_client.clone(), conf).await;
        self.sources.insert(source_id, source);
        Ok(())
    }

    async fn update_source(
        &mut self,
        source_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let old_conf: SourceConf = serde_json::from_value(old_conf)?;
        let new_conf: SourceConf = serde_json::from_value(new_conf)?;
        match self.sources.get_mut(source_id) {
            Some(mut source) => {
                source.update_conf(old_conf, new_conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_string())),
        }
    }

    async fn write_source_value(&mut self, source_id: String, value: Value) -> HaliaResult<()> {
        todo!()
    }

    async fn delete_source(&mut self, source_id: &String) -> HaliaResult<()> {
        match self.sources.remove(source_id) {
            Some((_, mut source)) => {
                source.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_string())),
        }
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(self.opcua_client.clone(), conf);
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: &String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let old_conf: SinkConf = serde_json::from_value(old_conf)?;
        let new_conf: SinkConf = serde_json::from_value(new_conf)?;
        match self.sinks.get_mut(sink_id) {
            Some(mut sink) => {
                sink.update_conf(old_conf, new_conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id.to_string())),
        }
    }

    async fn delete_sink(&mut self, sink_id: &String) -> HaliaResult<()> {
        match self.sinks.remove(sink_id) {
            Some((_, mut sink)) => {
                sink.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id.to_string())),
        }
    }

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.sources.get(source_id) {
            Some(source) => Ok(source.mb_tx.subscribe()),
            None => Err(HaliaError::NotFound(source_id.to_string())),
        }
    }

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_string())),
        }
    }

    async fn stop(&mut self) {
        for mut source in self.sources.iter_mut() {
            source.stop().await;
        }

        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        self.stop_signal_tx.send(()).unwrap();
        match self.opcua_client.read().await.as_ref() {
            Some(session) => match session.disconnect().await {
                Ok(_) => {
                    debug!("session disconnect success");
                }
                Err(e) => {
                    debug!("err code is :{}", e);
                }
            },
            None => {}
        }
    }
}

fn transfer_node_id(node_id: &types::devices::opcua::NodeId) -> NodeId {
    let value = node_id.identifier.value.clone();
    let identifier = match node_id.identifier.typ {
        types::devices::opcua::IdentifierType::Numeric => {
            let num: u32 = serde_json::from_value(value).unwrap();
            Identifier::Numeric(num)
        }
        types::devices::opcua::IdentifierType::String => {
            let s: opcua::types::UAString = serde_json::from_value(value).unwrap();
            Identifier::String(s)
        }
        types::devices::opcua::IdentifierType::Guid => {
            let guid: opcua::types::Guid = serde_json::from_value(value).unwrap();
            Identifier::Guid(guid)
        }
        types::devices::opcua::IdentifierType::ByteString => {
            let bs: opcua::types::ByteString = serde_json::from_value(value).unwrap();
            Identifier::ByteString(bs)
        }
    };

    NodeId {
        namespace: node_id.namespace,
        identifier,
    }
}
