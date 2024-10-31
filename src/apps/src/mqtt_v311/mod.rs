use std::{
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use rumqttc::{mqttbytes, AsyncClient, Event, Incoming, LastWill, MqttOptions, QoS};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{UnboundedReceiver, UnboundedSender},
        watch, RwLock,
    },
    task::JoinHandle,
};
use tracing::{error, warn};
use types::apps::{
    mqtt_client_v311::{Conf, Qos, SinkConf, SourceConf},
    SearchAppsItemRunningInfo,
};

use crate::{mqtt_client_ssl::get_ssl_config, App};

mod sink;
mod source;

pub struct MqttClient {
    _id: String,

    err: Arc<RwLock<Option<String>>>,
    stop_signal_tx: watch::Sender<()>,
    app_err_tx: broadcast::Sender<bool>,

    mqtt_client: Arc<AsyncClient>,

    sources: Arc<DashMap<String, Source>>,
    sinks: DashMap<String, Sink>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    rtt: AtomicU16,
}

struct JoinHandleData {
    conf: Conf,
    stop_signal_rx: watch::Receiver<()>,
    app_err_tx: broadcast::Sender<bool>,
    sources: Arc<DashMap<String, Source>>,
    app_err: Arc<RwLock<Option<String>>>,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: Conf = serde_json::from_value(conf).unwrap();
    let (app_err_tx, _) = broadcast::channel(16);
    let (stop_signal_tx, stop_signal_rx) = watch::channel(());

    let sources = Arc::new(DashMap::new());
    let app_err = Arc::new(RwLock::new(None));

    let join_handle_data = JoinHandleData {
        conf,
        stop_signal_rx,
        app_err_tx: app_err_tx.clone(),
        sources: sources.clone(),
        app_err: app_err.clone(),
    };
    let (join_handle, mqtt_client) = MqttClient::event_loop(join_handle_data);

    Box::new(MqttClient {
        _id: id,
        err: app_err,
        sources,
        sinks: DashMap::new(),
        stop_signal_tx,
        app_err_tx,
        join_handle: Some(join_handle),
        mqtt_client,
        rtt: AtomicU16::new(0),
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
            types::PlainOrBase64ValueType::Base64 => match &last_will.message.value {
                serde_json::Value::String(s) => {
                    BASE64_STANDARD.decode(s).map_err(|e| {
                        HaliaError::Common(format!("遗嘱信息base64解码错误: {}", e))
                    })?;
                }
                _ => {
                    return Err(HaliaError::Common(
                        "遗嘱信息base64编码时，value必须是字符串".to_owned(),
                    ));
                }
            },
        }
    }

    if conf.ssl_enable {
        if conf.ssl_conf.is_none() {
            return Err(HaliaError::Common("ssl_conf is required".to_owned()));
        }
    }

    Ok(())
}

pub async fn process_source_conf(id: &String, conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SourceConf = serde_json::from_value(conf.clone())?;
    Source::process_conf(id, &conf).await
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(&conf)?;
    Ok(())
}

impl MqttClient {
    fn event_loop(
        mut join_handle_data: JoinHandleData,
    ) -> (JoinHandle<JoinHandleData>, Arc<AsyncClient>) {
        let mut mqtt_options = MqttOptions::new(
            &join_handle_data.conf.client_id,
            &join_handle_data.conf.host,
            join_handle_data.conf.port,
        );
        mqtt_options.set_keep_alive(Duration::from_secs(join_handle_data.conf.keep_alive));
        mqtt_options.set_clean_session(join_handle_data.conf.clean_session);

        match join_handle_data.conf.auth_method {
            types::apps::mqtt_client_v311::AuthMethod::None => {}
            types::apps::mqtt_client_v311::AuthMethod::Password => {
                mqtt_options.set_credentials(
                    &join_handle_data
                        .conf
                        .auth_password
                        .as_ref()
                        .unwrap()
                        .username,
                    &join_handle_data
                        .conf
                        .auth_password
                        .as_ref()
                        .unwrap()
                        .password,
                );
            }
        }

        if join_handle_data.conf.ssl_enable {
            let config = get_ssl_config(&join_handle_data.conf.ssl_conf.as_ref().unwrap());
            let transport =
                rumqttc::Transport::Tls(rumqttc::TlsConfiguration::Rustls(Arc::new(config)));
            mqtt_options.set_transport(transport);
        }

        if join_handle_data.conf.last_will_enable {
            let message: Vec<u8> = join_handle_data
                .conf
                .last_will
                .as_ref()
                .unwrap()
                .message
                .clone()
                .into();
            mqtt_options.set_last_will(LastWill {
                topic: join_handle_data
                    .conf
                    .last_will
                    .as_ref()
                    .unwrap()
                    .topic
                    .clone(),
                message: message.into(),
                qos: transfer_qos(&join_handle_data.conf.last_will.as_ref().unwrap().qos),
                retain: join_handle_data.conf.last_will.as_ref().unwrap().retain,
            });
        }

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 16);

        let join_handle = tokio::spawn(async move {
            let mut err = false;
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    event = event_loop.poll() => {
                        Self::handle_event(event, &join_handle_data.sources, &join_handle_data.app_err, &mut err, &join_handle_data.app_err_tx).await;
                    }
                }
            }
        });

        (join_handle, Arc::new(client))
    }

    async fn handle_event(
        event: Result<Event, rumqttc::ConnectionError>,
        sources: &Arc<DashMap<String, Source>>,
        device_err: &Arc<RwLock<Option<String>>>,
        err: &mut bool,
        app_err_tx: &broadcast::Sender<bool>,
    ) {
        match event {
            Ok(Event::Incoming(Incoming::Publish(p))) => {
                if *err {
                    *err = false;
                    _ = app_err_tx.send(false);
                    *device_err.write().await = None;
                }
                for mut source in sources.iter_mut() {
                    if matches(&p.topic, &source.conf.topic) {
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
            Ok(_event) => {
                if *err {
                    *err = false;
                    _ = app_err_tx.send(false);
                    *device_err.write().await = None;
                }
            }
            Err(e) => {
                if !*err {
                    *err = true;
                    _ = app_err_tx.send(true);
                    *device_err.write().await = Some(e.to_string());
                }
            }
        }
    }
}

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
    async fn read_running_info(&self) -> SearchAppsItemRunningInfo {
        SearchAppsItemRunningInfo {
            err: self.err.read().await.clone(),
            rtt: self.rtt.load(Ordering::SeqCst),
        }
    }

    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        self.stop_signal_tx.send(()).unwrap();

        let new_conf: Conf = serde_json::from_value(new_conf)?;
        let mut join_handle_data = self.join_handle.take().unwrap().await.unwrap();
        join_handle_data.conf = new_conf;

        let (join_hanlde, mqtt_client) = Self::event_loop(join_handle_data);
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
            .subscribe(&source.conf.topic, transfer_qos(&source.conf.qos))
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

        if let Err(e) = self.mqtt_client.unsubscribe(old_conf.topic).await {
            error!("unsubscribe err:{e}");
        }
        if let Err(e) = self
            .mqtt_client
            .subscribe(&new_conf.topic, transfer_qos(&new_conf.qos))
            .await
        {
            error!("subscribe err:{e}");
        }

        source.conf = new_conf;

        Ok(())
    }

    async fn delete_source(&mut self, source_id: String) -> HaliaResult<()> {
        let (_, source) = self
            .sources
            .remove(&source_id)
            .ok_or(HaliaError::NotFound(source_id))?;

        if let Err(e) = self.mqtt_client.unsubscribe(source.conf.topic).await {
            error!("unsubscribe err:{e}");
        }

        Ok(())
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        // for sink in self.sinks.iter() {
        // sink.check_duplicate(&req.base, &ext_conf)?;
        // }

        let sink = Sink::new(conf, self.mqtt_client.clone(), self.app_err_tx.subscribe()).await;
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
            Some(mut sink) => sink.update_conf(old_conf, new_conf).await,
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

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<UnboundedReceiver<RuleMessageBatch>> {
        match self.sources.get_mut(source_id) {
            Some(mut source) => Ok(source.get_rx()),
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn get_sink_tx(
        &self,
        sink_id: &String,
    ) -> HaliaResult<UnboundedSender<RuleMessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}
