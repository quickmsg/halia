use std::{
    io::{BufReader, Cursor},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use rumqttc::{
    mqttbytes, tokio_rustls::rustls::ClientConfig, AsyncClient, Event, Incoming, LastWill,
    MqttOptions, QoS,
};
use rustls_pemfile::Item;
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
};
use tracing::{debug, error, warn};
use types::apps::mqtt_client_v311::{MqttClientConf, Qos, SinkConf, SourceConf};

use crate::App;

mod sink;
mod source;

pub struct MqttClient {
    _id: String,

    err: Arc<RwLock<Option<String>>>,
    stop_signal_tx: mpsc::Sender<()>,
    app_err_tx: broadcast::Sender<bool>,

    sources: Arc<DashMap<String, Source>>,
    sinks: DashMap<String, Sink>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
}

struct JoinHandleData {
    mqtt_client: Arc<AsyncClient>,
    stop_signal_rx: mpsc::Receiver<()>,
    app_err_tx: broadcast::Sender<bool>,
    sources: Arc<DashMap<String, Source>>,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: MqttClientConf = serde_json::from_value(conf).unwrap();

    let (app_err_tx, _) = broadcast::channel(16);

    let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);

    let sources = Arc::new(DashMap::new());
    let app_err = Arc::new(RwLock::new(None));
    let join_handle = MqttClient::start(
        conf,
        sources.clone(),
        stop_signal_rx,
        app_err_tx.clone(),
        app_err.clone(),
    );

    Box::new(MqttClient {
        _id: id,
        err: app_err,
        sources,
        sinks: DashMap::new(),
        stop_signal_tx,
        app_err_tx,
        join_handle: Some(join_handle),
    })
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: MqttClientConf = serde_json::from_value(conf.clone())?;

    if let Some(last_will) = &conf.last_will {
        match &last_will.message.typ {
            types::ValueType::String => {}
            types::ValueType::Bytes => {
                BASE64_STANDARD
                    .decode(&last_will.message.value)
                    .map_err(|e| HaliaError::Common(format!("遗嘱信息base64解码错误: {}", e)))?;
            }
        }
    }

    Ok(())
}

pub fn validate_source_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SourceConf = serde_json::from_value(conf.clone())?;
    Source::validate_conf(&conf)?;
    Ok(())
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(&conf)?;
    Ok(())
}

impl MqttClient {
    fn start(
        conf: MqttClientConf,
        sources: Arc<DashMap<String, Source>>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        app_err_tx: broadcast::Sender<bool>,
        device_err: Arc<RwLock<Option<String>>>,
    ) -> (
        JoinHandle<(
            mpsc::Receiver<()>,
            broadcast::Sender<bool>,
            Arc<DashMap<String, Source>>,
        )>,
    ) {
        let mut mqtt_options = MqttOptions::new(&conf.client_id, &conf.host, conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(conf.keep_alive));
        mqtt_options.set_clean_session(conf.clean_session);
        match (&conf.auth.username, &conf.auth.password) {
            (None, None) => {}
            (None, Some(password)) => {
                mqtt_options.set_credentials("", password);
            }
            (Some(username), None) => {
                mqtt_options.set_credentials(username, "");
            }
            (Some(username), Some(password)) => {
                mqtt_options.set_credentials(username, password);
            }
        };

        if conf.ssl.enable {
            let mut root_cert_store = rumqttc::tokio_rustls::rustls::RootCertStore::empty();
            let certificate_result = rustls_native_certs::load_native_certs();
            if certificate_result.errors.is_empty() {
                root_cert_store.add_parsable_certificates(certificate_result.certs);
            }
            match conf.ssl.ca_cert {
                Some(ca_cert) => {
                    let ca_cert = ca_cert.into_bytes();
                    let certs = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(ca_cert)))
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    for cert in certs {
                        root_cert_store.add(cert).unwrap();
                    }
                }
                None => {}
            }

            let client_config = ClientConfig::builder().with_root_certificates(root_cert_store);
            match (conf.ssl.client_cert, conf.ssl.client_key) {
                (Some(client_cert), Some(client_key)) => {
                    let client_cert = client_cert.into_bytes();
                    let client_key = client_key.into_bytes();
                    let client_certs =
                        rustls_pemfile::certs(&mut BufReader::new(Cursor::new(client_cert)))
                            .collect::<Result<Vec<_>, _>>()
                            .unwrap();

                    let mut key_buffer = BufReader::new(Cursor::new(client_key));
                    let key = loop {
                        let item = rustls_pemfile::read_one(&mut key_buffer).unwrap();
                        match item {
                            Some(Item::Sec1Key(key)) => {
                                break key.into();
                            }
                            Some(Item::Pkcs1Key(key)) => {
                                break key.into();
                            }
                            Some(Item::Pkcs8Key(key)) => {
                                break key.into();
                            }
                            None => {
                                panic!("no valid key in chain");
                            }
                            _ => {}
                        }
                    };
                    let config = client_config
                        .with_client_auth_cert(client_certs, key)
                        .unwrap();

                    let transport = rumqttc::Transport::Tls(rumqttc::TlsConfiguration::Rustls(
                        Arc::new(config),
                    ));
                    mqtt_options.set_transport(transport);
                }
                _ => {}
            }

            if conf.ssl.verify {}
        }

        if let Some(last_will) = &conf.last_will {
            let message = match last_will.message.typ {
                types::ValueType::String => last_will.message.value.clone().into(),
                types::ValueType::Bytes => {
                    BASE64_STANDARD.decode(&last_will.message.value).unwrap()
                }
            };
            mqtt_options.set_last_will(LastWill {
                topic: last_will.topic.clone(),
                message: message.into(),
                qos: qos_to_v311(&last_will.qos),
                retain: last_will.retain,
            });
        }

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 16);

        let join_handle = tokio::spawn(async move {
            let mut err = false;
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, app_err_tx, sources);
                    }

                    event = event_loop.poll() => {
                        Self::handle_event(event, &sources, &device_err, &mut err, &app_err_tx).await;
                    }
                }
            }
        });

        (HaliaMqttClient::V311(Arc::new(client)), join_handle)
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
                debug!("topic:{}, payload:{:?}", p.topic, p.payload);
                if *err {
                    *err = false;
                    _ = app_err_tx.send(false);
                    *device_err.write().await = None;
                }
                match MessageBatch::from_json(p.payload) {
                    Ok(msg) => {
                        for source in sources.iter_mut() {
                            if matches(&source.conf.topic, &p.topic) {
                                if source.mb_tx.receiver_count() > 0 {
                                    if let Err(e) = source.mb_tx.send(msg.clone()) {
                                        warn!("{}", e);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => error!("Failed to decode msg:{}", e),
                }
            }
            Ok(_) => {
                debug!("v311 event ok null");
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
    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        self.stop_signal_tx.send(()).await.unwrap();

        let join_handle_data = self.join_handle.take().unwrap().await.unwrap();
        let new_conf: MqttClientConf = serde_json::from_value(new_conf)?;
        let (halia_mqtt_client, join_hanlde) = Self::start(
            new_conf.unwrap(),
            sources,
            stop_signal_rx,
            app_err_tx,
            self.err.clone(),
        );
        self.halia_mqtt_client = halia_mqtt_client;
        self.join_handle = Some(join_hanlde);

        for mut sink in self.sinks.iter_mut() {
            sink.restart(self.halia_mqtt_client.clone()).await;
        }

        Ok(())
    }

    async fn stop(&mut self) {
        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
        match &self.halia_mqtt_client {
            HaliaMqttClient::V311(client_v311) => {
                if let Err(e) = client_v311.disconnect().await {
                    warn!("client disconnect err:{e}");
                }
            }
            HaliaMqttClient::V50(client_v50) => {
                let _ = client_v50.disconnect().await;
            }
        }
        self.stop_signal_tx.send(()).await.unwrap();
    }

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;

        let source = Source::new(conf);
        match &self.halia_mqtt_client {
            HaliaMqttClient::V311(client_v311) => {
                if let Err(e) = client_v311
                    .subscribe(&source.conf.topic, qos_to_v311(&source.conf.qos))
                    .await
                {
                    error!("client subscribe err:{e}");
                }
            }
            HaliaMqttClient::V50(client_v50) => {
                if let Err(e) = client_v50
                    .subscribe(&source.conf.topic, qos_to_v50(&source.conf.qos))
                    .await
                {
                    error!("client subscribe err:{e}");
                }
            }
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

        match &self.halia_mqtt_client {
            HaliaMqttClient::V311(client_v311) => {
                if let Err(e) = client_v311.unsubscribe(old_conf.topic).await {
                    error!("unsubscribe err:{e}");
                }
                if let Err(e) = client_v311
                    .subscribe(&new_conf.topic, qos_to_v311(&new_conf.qos))
                    .await
                {
                    error!("subscribe err:{e}");
                }
            }
            HaliaMqttClient::V50(client_v50) => {
                if let Err(e) = client_v50.unsubscribe(old_conf.topic).await {
                    error!("unsubscribe err:{e}");
                }
                if let Err(e) = client_v50
                    .subscribe(&new_conf.topic, qos_to_v50(&new_conf.qos))
                    .await
                {
                    error!("subscribe err:{e}");
                }
            }
        }

        source.conf = new_conf;

        Ok(())
    }

    async fn delete_source(&mut self, source_id: String) -> HaliaResult<()> {
        let (_, source) = self
            .sources
            .remove(&source_id)
            .ok_or(HaliaError::NotFound(source_id))?;

        if let Err(e) = client_v311.unsubscribe(source.conf.topic).await {
            error!("unsubscribe err:{e}");
        }

        Ok(())
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf)?;
        // for sink in self.sinks.iter() {
        // sink.check_duplicate(&req.base, &ext_conf)?;
        // }

        let sink = Sink::new(
            conf,
            self.halia_mqtt_client.clone(),
            self.app_err_tx.subscribe(),
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
            Some(mut sink) => sink.update(old_conf, new_conf).await,
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
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self.sources.get(source_id) {
            Some(source) => Ok(source.mb_tx.subscribe()),
            None => Err(HaliaError::NotFound(source_id.to_owned())),
        }
    }

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}
