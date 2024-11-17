use std::{
    sync::{atomic::AtomicU16, Arc},
    time::Duration,
};

use anyhow::{bail, Result};
use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use opcua_protocol::{
    client::{ClientBuilder, IdentityToken, Session},
    types::{EndpointDescription, Identifier, MonitoringMode, NodeId, StatusCode},
};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{mpsc, watch, RwLock},
    task::JoinHandle,
    time,
};
use tracing::{debug, warn};
use types::{
    devices::{
        device::opcua::{OpcuaConf, SinkConf, SourceConf},
        device_template::opcua::{CustomizeConf, TemplateConf},
    },
    Value,
};

use crate::{add_device_running_count, sub_device_running_count, Device};

mod sink;
mod source;
pub(crate) mod template;

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
    pub opcua_conf: OpcuaConf,
    pub opcua_client: Arc<RwLock<Option<Arc<Session>>>>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub err: Arc<RwLock<Option<String>>>,
}

pub fn new_by_customize_conf(id: String, conf: serde_json::Value) -> Box<dyn Device> {
    let opcua_conf: OpcuaConf = serde_json::from_value(conf).unwrap();
    new(id, opcua_conf)
}

pub fn new_by_template_conf(
    id: String,
    customize_conf: serde_json::Value,
    template_conf: serde_json::Value,
) -> Box<dyn Device> {
    let opcua_conf = Opcua::get_device_conf(customize_conf, template_conf).unwrap();
    new(id, opcua_conf)
}

fn new(id: String, opcua_conf: OpcuaConf) -> Box<dyn Device> {
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());

    let opcua_client: Arc<RwLock<Option<Arc<Session>>>> = Arc::new(RwLock::new(None));

    let err = Arc::new(RwLock::new(None));
    let join_handle_data = JoinHandleData {
        id,
        opcua_conf,
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
    async fn connect(conf: &OpcuaConf) -> Result<(Arc<Session>, JoinHandle<StatusCode>)> {
        let mut client = ClientBuilder::new()
            .application_name("Halia")
            .application_uri("https://halia.com")
            .product_uri("https://halia.com")
            .trust_server_certs(true)
            .create_sample_keypair(true)
            .client()
            .unwrap();

        let url = format!("opc.tcp://{}:{}{}", conf.host, conf.port, conf.path);
        let endpoint: EndpointDescription = EndpointDescription::from(url.as_ref());

        let user_identity_token = match conf.auth_method {
            types::devices::device::opcua::AuthMethod::Anonymous => IdentityToken::Anonymous,
            types::devices::device::opcua::AuthMethod::Username => {
                let username = conf.auth_username.as_ref().unwrap();
                IdentityToken::UserName(username.username.clone(), username.password.clone())
            }
            types::devices::device::opcua::AuthMethod::X509 => {
                let certificate = conf.auth_certificate.as_ref().unwrap();
                // 将内容写入内存
                IdentityToken::X509(todo!(), todo!())
            }
        };
        let (session, event_loop) = match client
            .new_session_from_endpoint(endpoint, user_identity_token)
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
                match Opcua::connect(&join_handle_data.opcua_conf).await {
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
                            time::sleep(Duration::from_secs(join_handle_data.opcua_conf.reconnect));
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

    fn get_device_conf(
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<OpcuaConf> {
        let customize_conf: CustomizeConf = serde_json::from_value(customize_conf)?;
        let template_conf: TemplateConf = serde_json::from_value(template_conf)?;

        Ok(OpcuaConf {
            host: customize_conf.host,
            port: customize_conf.port,
            path: template_conf.path,
            reconnect: template_conf.reconnect,
            auth_method: template_conf.auth_method,
            auth_username: template_conf.auth_username,
            auth_certificate: template_conf.auth_certificate,
        })
    }

    async fn update_conf(&mut self, opcua_conf: OpcuaConf) {
        self.stop_signal_tx.send(()).unwrap();
        let mut join_handle_data = self.join_handle.take().unwrap().await.unwrap();
        join_handle_data.opcua_conf = opcua_conf;
        let join_handle = Self::event_loop(join_handle_data);
        self.join_handle = Some(join_handle);
    }

    fn get_source_conf(
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<SourceConf> {
        let customize_conf: SourceConf = serde_json::from_value(customize_conf).unwrap();
        let template_conf: SourceConf = serde_json::from_value(template_conf).unwrap();
        Ok(SourceConf {
            typ: customize_conf.typ,
            group: customize_conf.group.or(template_conf.group),
            subscription: customize_conf.subscription.or(template_conf.subscription),
            monitored_item: customize_conf
                .monitored_item
                .or(template_conf.monitored_item),
        })
    }
}

#[async_trait]
impl Device for Opcua {
    async fn read_err(&self) -> Option<String> {
        self.err.read().await.clone()
    }

    async fn read_source_err(&self, _source_id: &String) -> Option<String> {
        None
    }

    async fn read_sink_err(&self, _sink_id: &String) -> Option<String> {
        None
    }

    async fn update_customize_conf(&mut self, conf: serde_json::Value) -> HaliaResult<()> {
        let opcua_conf: OpcuaConf = serde_json::from_value(conf)?;
        Self::update_conf(&mut self, opcua_conf).await;
        Ok(())
    }

    async fn update_template_conf(
        &mut self,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let opcua_conf = Self::get_device_conf(customize_conf, template_conf)?;
        Self::update_conf(&mut self, opcua_conf).await;

        Ok(())
    }

    async fn create_customize_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        let source = Source::new(self.opcua_client.clone(), conf).await;
        self.sources.insert(source_id, source);
        Ok(())
    }

    async fn create_template_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        let source = Source::new(self.opcua_client.clone(), conf).await;
        self.sources.insert(source_id, source);
        Ok(())
    }

    async fn update_customize_source(
        &mut self,
        source_id: &String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        match self.sources.get_mut(source_id) {
            Some(mut source) => {
                source.update_conf(conf).await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(source_id.to_string())),
        }
    }

    async fn update_template_source(
        &mut self,
        source_id: &String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        // TODO
        let conf: SourceConf = serde_json::from_value(customize_conf)?;
        match self.sources.get_mut(source_id) {
            Some(mut source) => {
                source.update_conf(conf).await;
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

    async fn create_customize_sink(
        &mut self,
        sink_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(self.opcua_client.clone(), conf);
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn create_template_sink(
        &mut self,
        sink_id: String,
        conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(self.opcua_client.clone(), conf);
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_customize_sink(
        &mut self,
        sink_id: &String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        // let new_conf: SinkConf = serde_json::from_value(new_conf)?;
        // match self.sinks.get_mut(sink_id) {
        //     Some(mut sink) => {
        //         sink.update_conf(old_conf, new_conf).await;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound(sink_id.to_string())),
        // }
        todo!()
    }

    async fn update_template_sink(
        &mut self,
        sink_id: &String,
        customize_conf: serde_json::Value,
        template_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        // let new_conf: SinkConf = serde_json::from_value(new_conf)?;
        // match self.sinks.get_mut(sink_id) {
        //     Some(mut sink) => {
        //         sink.update_conf(old_conf, new_conf).await;
        //         Ok(())
        //     }
        //     None => Err(HaliaError::NotFound(sink_id.to_string())),
        // }
        todo!()
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

    async fn get_source_rxs(
        &self,
        source_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>> {
        // match self.sources.get(source_id) {
        //     Some(source) => Ok(source.mb_tx.subscribe()),
        //     None => Err(HaliaError::NotFound(source_id.to_string())),
        // }
        todo!()
    }

    async fn get_sink_txs(
        &self,
        sink_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedSender<RuleMessageBatch>>> {
        todo!()
        // match self.sinks.get(sink_id) {
        //     Some(sink) => Ok(sink.mb_tx.clone()),
        //     None => Err(HaliaError::NotFound(sink_id.to_string())),
        // }
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

fn transfer_node_id(node_id: &types::devices::device::opcua::NodeId) -> NodeId {
    let value = node_id.identifier.value.clone();
    let identifier = match node_id.identifier.typ {
        types::devices::device::opcua::IdentifierType::Numeric => {
            let num: u32 = serde_json::from_value(value).unwrap();
            Identifier::Numeric(num)
        }
        types::devices::device::opcua::IdentifierType::String => {
            let s: opcua_protocol::types::UAString = serde_json::from_value(value).unwrap();
            Identifier::String(s)
        }
        types::devices::device::opcua::IdentifierType::Guid => {
            let guid: opcua_protocol::types::Guid = serde_json::from_value(value).unwrap();
            Identifier::Guid(guid)
        }
        types::devices::device::opcua::IdentifierType::Opaque => {
            let bs: opcua_protocol::types::ByteString = serde_json::from_value(value).unwrap();
            Identifier::ByteString(bs)
        }
    };

    NodeId {
        namespace: node_id.namespace,
        identifier,
    }
}

fn transfer_monitoring_node(
    mode: &types::devices::device::opcua::MonitoringMode,
) -> MonitoringMode {
    match mode {
        types::devices::device::opcua::MonitoringMode::Disabled => MonitoringMode::Disabled,
        types::devices::device::opcua::MonitoringMode::Sampling => MonitoringMode::Sampling,
        types::devices::device::opcua::MonitoringMode::Reporting => MonitoringMode::Reporting,
    }
}
