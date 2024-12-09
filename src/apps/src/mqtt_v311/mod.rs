use std::{
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use futures::lock::BiLock;
use halia_derive::ResourceErr;
use message::RuleMessageBatch;
use rumqttc::{mqttbytes, AsyncClient, Event, Incoming, LastWill, MqttOptions, QoS};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use tracing::{debug, error, warn};
use types::apps::mqtt_client_v311::{Conf, Qos, SinkConf, SourceConf};
use utils::ErrorManager;

use crate::{mqtt_client_ssl::get_ssl_config, App};

mod sink;
mod source;

#[derive(ResourceErr)]
pub struct MqttClient {
    err: BiLock<Option<Arc<String>>>,
    stop_signal_tx: watch::Sender<()>,
    mqtt_client: Arc<AsyncClient>,
    sources: Arc<DashMap<String, Source>>,
    sinks: DashMap<String, Sink>,
    join_handle: Option<JoinHandle<TaskLoop>>,

    mqtt_status: Arc<AtomicBool>,
}

struct TaskLoop {
    app_conf: Conf,
    stop_signal_rx: watch::Receiver<()>,
    sources: Arc<DashMap<String, Source>>,
    error_manager: ErrorManager,
    mqtt_status: Arc<AtomicBool>,
}

impl TaskLoop {
    fn new(
        app_id: String,
        app_conf: Conf,
        app_err: BiLock<Option<Arc<String>>>,
        stop_signal_rx: watch::Receiver<()>,
        sources: Arc<DashMap<String, Source>>,
        mqtt_status: Arc<AtomicBool>,
    ) -> Self {
        let error_manager = ErrorManager::new(
            utils::error_manager::ResourceType::App,
            app_id.clone(),
            app_err,
        );
        Self {
            app_conf,
            stop_signal_rx,
            sources,
            error_manager,
            mqtt_status,
        }
    }

    fn start(mut self) -> (JoinHandle<TaskLoop>, Arc<AsyncClient>) {
        let mut mqtt_options = MqttOptions::new(
            &self.app_conf.client_id,
            &self.app_conf.host,
            self.app_conf.port,
        );
        mqtt_options.set_keep_alive(Duration::from_secs(self.app_conf.keep_alive));
        mqtt_options.set_clean_session(self.app_conf.clean_session);

        match self.app_conf.auth_method {
            types::apps::mqtt_client_v311::AuthMethod::None => {}
            types::apps::mqtt_client_v311::AuthMethod::Password => {
                mqtt_options.set_credentials(
                    &self.app_conf.auth_password.as_ref().unwrap().username,
                    &self.app_conf.auth_password.as_ref().unwrap().password,
                );
            }
        }

        if self.app_conf.ssl_enable {
            let config = get_ssl_config(&self.app_conf.ssl_conf.as_ref().unwrap());
            let transport =
                rumqttc::Transport::Tls(rumqttc::TlsConfiguration::Rustls(Arc::new(config)));
            mqtt_options.set_transport(transport);
        }

        if self.app_conf.last_will_enable {
            let message: Vec<u8> = self
                .app_conf
                .last_will
                .as_ref()
                .unwrap()
                .message
                .clone()
                .into();
            mqtt_options.set_last_will(LastWill {
                topic: self.app_conf.last_will.as_ref().unwrap().topic.clone(),
                message: message.into(),
                qos: transfer_qos(&self.app_conf.last_will.as_ref().unwrap().qos),
                retain: self.app_conf.last_will.as_ref().unwrap().retain,
            });
        }

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 256);

        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    event = event_loop.poll() => {
                        self.handle_event(event).await;
                    }
                }
            }
        });

        (join_handle, Arc::new(client))
    }

    async fn handle_event(&mut self, event: Result<Event, rumqttc::ConnectionError>) {
        match event {
            Ok(event) => {
                let status_changed = self.error_manager.set_ok().await;
                if status_changed {
                    self.mqtt_status
                        .store(true, std::sync::atomic::Ordering::Relaxed);
                }

                match event {
                    Event::Incoming(Incoming::Publish(p)) => {
                        for mut source in self.sources.iter_mut() {
                            if matches(&p.topic, &source.source_conf.topic) {
                                let mb = match source.decoder.decode(p.payload.clone()) {
                                    Ok(mb) => mb,
                                    Err(e) => {
                                        warn!("decode err :{}", e);
                                        break;
                                    }
                                };

                                match source.mb_txs.len() {
                                    0 => {}
                                    1 => {
                                        let mb = RuleMessageBatch::Owned(mb);
                                        if let Err(e) = source.mb_txs[0].send(mb) {
                                            warn!("send err :{}", e);
                                            source.mb_txs.remove(0);
                                        }
                                    }
                                    _ => {
                                        let mb = RuleMessageBatch::Arc(Arc::new(mb));
                                        source.mb_txs.retain(|tx| tx.send(mb.clone()).is_ok());
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            Err(err) => {
                let err = Arc::new(err.to_string());
                let status_changed = self.error_manager.set_err(err).await;
                if status_changed {
                    self.mqtt_status
                        .store(false, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
    }
}

pub fn new(app_id: String, conf: serde_json::Value) -> Box<dyn App> {
    debug!("here");
    let conf: Conf = serde_json::from_value(conf).unwrap();
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());

    let sources = Arc::new(DashMap::new());
    let (app_err1, app_err2) = BiLock::new(None);

    let mqtt_status = Arc::new(AtomicBool::new(false));
    let task_loop = TaskLoop::new(
        app_id,
        conf,
        app_err1,
        stop_signal_rx,
        sources.clone(),
        mqtt_status.clone(),
    );
    let (join_handle, mqtt_client) = task_loop.start();

    Box::new(MqttClient {
        err: app_err2,
        sources,
        sinks: DashMap::new(),
        stop_signal_tx,
        join_handle: Some(join_handle),
        mqtt_client,
        mqtt_status,
    })
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: Conf = serde_json::from_value(conf.clone())?;

    match conf.auth_method {
        types::apps::mqtt_client_v311::AuthMethod::None => {}
        types::apps::mqtt_client_v311::AuthMethod::Password => {
            if conf.auth_password.is_none() {
                return Err(HaliaError::Common("auth_password is required".to_owned()));
            }
        }
    }

    if let Some(last_will) = &conf.last_will {
        match &last_will.message.typ {
            types::PlainOrBase64ValueType::Plain => {}
            types::PlainOrBase64ValueType::Base64 => {
                BASE64_STANDARD
                    .decode(&last_will.message.value)
                    .map_err(|e| HaliaError::Common(format!("遗嘱信息base64解码错误: {}", e)))?;
            }
        }
    }

    if conf.ssl_enable {
        if conf.ssl_conf.is_none() {
            return Err(HaliaError::Common("ssl_conf is required".to_owned()));
        }
    }

    Ok(())
}

pub async fn process_source_conf(
    app_id: &String,
    source_id: &String,
    conf: &serde_json::Value,
) -> HaliaResult<()> {
    let conf: SourceConf = serde_json::from_value(conf.clone())?;
    Source::process_conf(app_id, source_id, &conf).await
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(&conf)?;
    Ok(())
}

impl MqttClient {}

pub fn matches(topic: &str, filter: &str) -> bool {
    if !topic.is_empty() && topic[..1].contains('$') {
        return false;
    }

    let mut topics = topic.split('/');
    let mut filters = filter.split('/');

    for f in filters.by_ref() {
        // "#" being the last element is validated by the broker with 'valid_filter'
        if f == "#" {
            return true;
        }

        // filter still has remaining elements
        // filter = a/b/c/# should match topci = a/b/c
        // filter = a/b/c/d should not match topic = a/b/c
        let top = topics.next();
        match top {
            Some("#") => return false,
            Some(_) if f == "+" => continue,
            Some(t) if f != t => return false,
            Some(_) => continue,
            None => return false,
        }
    }

    // topic has remaining elements and filter's last element isn't "#"
    if topics.next().is_some() {
        return false;
    }

    true
}

fn transfer_qos(qos: &Qos) -> mqttbytes::QoS {
    match qos {
        Qos::AtMostOnce => QoS::AtMostOnce,
        Qos::AtLeastOnce => QoS::AtLeastOnce,
        Qos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

#[async_trait]
impl App for MqttClient {
    async fn read_app_err(&self) -> Option<Arc<String>> {
        self.read_err().await
    }

    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        self.stop_signal_tx.send(()).unwrap();

        let new_conf: Conf = serde_json::from_value(new_conf)?;
        let mut task_loop = self.join_handle.take().unwrap().await.unwrap();
        task_loop.app_conf = new_conf;
        let (join_hanlde, mqtt_client) = task_loop.start();
        self.mqtt_client = mqtt_client;
        self.join_handle = Some(join_hanlde);

        for mut sink in self.sinks.iter_mut() {
            sink.update_mqtt_client(self.mqtt_client.clone()).await;
        }

        Ok(())
    }

    async fn stop(&mut self) {
        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
        self.stop_signal_tx.send(()).unwrap();
        if let Err(e) = self.mqtt_client.disconnect().await {
            warn!("client disconnect err:{e}");
        }
    }

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;
        let source = Source::new(conf).await;
        if let Err(e) = self
            .mqtt_client
            .subscribe(
                &source.source_conf.topic,
                transfer_qos(&source.source_conf.qos),
            )
            .await
        {
            error!("client subscribe err:{e}");
        }
        self.sources.insert(source_id, source);

        Ok(())
    }

    async fn update_source(
        &mut self,
        source_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let old_conf: SourceConf = serde_json::from_value(old_conf)?;
        let new_conf: SourceConf = serde_json::from_value(new_conf)?;

        let mut source = self
            .sources
            .get_mut(&source_id)
            .ok_or(HaliaError::NotFound(source_id.to_owned()))?;

        if old_conf.topic != new_conf.topic {
            if let Err(e) = self.mqtt_client.unsubscribe(&old_conf.topic).await {
                error!("unsubscribe err:{e}");
            }
            if let Err(e) = self
                .mqtt_client
                .subscribe(&new_conf.topic, transfer_qos(&new_conf.qos))
                .await
            {
                error!("subscribe err:{e}");
            }
        }

        // TODO schema的删除问题
        if old_conf.decode_type != new_conf.decode_type || old_conf.schema_id != new_conf.schema_id
        {
            let decoder = schema::new_decoder(&new_conf.decode_type, &new_conf.schema_id)
                .await
                .unwrap();
            source.decoder = decoder;
        }

        source.source_conf = new_conf;

        Ok(())
    }

    async fn delete_source(&mut self, source_id: String) -> HaliaResult<()> {
        let (_, source) = self
            .sources
            .remove(&source_id)
            .ok_or(HaliaError::NotFound(source_id))?;

        if let Err(e) = self.mqtt_client.unsubscribe(source.source_conf.topic).await {
            error!("unsubscribe err:{e}");
        }

        Ok(())
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let sink_conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(
            sink_conf,
            self.mqtt_client.clone(),
            self.mqtt_status.clone(),
        )
        .await;
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let old_conf: SinkConf = serde_json::from_value(old_conf)?;
        let new_conf: SinkConf = serde_json::from_value(new_conf)?;

        match self.sinks.get_mut(&sink_id) {
            Some(mut sink) => Ok(sink.update_conf(old_conf, new_conf).await),
            None => Err(HaliaError::NotFound(sink_id)),
        }
    }

    async fn delete_sink(&mut self, sink_id: String) -> HaliaResult<()> {
        match self.sinks.remove(&sink_id) {
            Some((_, mut sink)) => {
                sink.stop().await;
                Ok(())
            }
            None => Err(HaliaError::NotFound(sink_id)),
        }
    }

    async fn get_source_rxs(
        &self,
        source_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<UnboundedReceiver<RuleMessageBatch>>> {
        match self.sources.get_mut(source_id) {
            Some(mut source) => Ok(source.get_rxs(cnt)),
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn get_sink_txs(
        &self,
        sink_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<UnboundedSender<RuleMessageBatch>>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.get_txs(cnt)),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}
