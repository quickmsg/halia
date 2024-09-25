use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use base64::{prelude::BASE64_STANDARD, Engine as _};
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use rumqttc::{
    mqttbytes, v5, AsyncClient, Event, Incoming, LastWill, MqttOptions, QoS, TlsConfiguration,
    Transport,
};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
};
use tracing::{error, warn};
use types::apps::mqtt_client::{MqttClientConf, MqttClientV311Conf, Qos, SinkConf, SourceConf};

use crate::App;

mod sink;
mod source;

pub struct MqttClient {
    pub id: String,

    err: Arc<RwLock<Option<String>>>,
    stop_signal_tx: mpsc::Sender<()>,
    app_err_tx: broadcast::Sender<bool>,

    sources: Arc<DashMap<String, Source>>,
    sinks: DashMap<String, Sink>,
    halia_mqtt_client: HaliaMqttClient,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            broadcast::Sender<bool>,
            Arc<DashMap<String, Source>>,
        )>,
    >,
}

#[derive(Clone)]
pub(crate) enum HaliaMqttClient {
    V311(Arc<AsyncClient>),
    V50(Arc<v5::AsyncClient>),
}

pub fn new(app_id: String, conf: serde_json::Value) -> HaliaResult<Box<dyn App>> {
    let conf: MqttClientConf = serde_json::from_value(conf)?;

    let (app_err_tx, _) = broadcast::channel(16);

    let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);

    let sources = Arc::new(DashMap::new());
    let app_err = Arc::new(RwLock::new(None));
    let (halia_mqtt_client, join_handle) = match conf.version {
        types::apps::mqtt_client::Version::V311 => MqttClient::start_v311(
            conf.v311.unwrap(),
            sources.clone(),
            stop_signal_rx,
            app_err_tx.clone(),
            app_err.clone(),
        ),
        types::apps::mqtt_client::Version::V50 => todo!(),
    };

    let mqtt_client = MqttClient {
        id: app_id,
        err: app_err,
        sources,
        sinks: DashMap::new(),
        halia_mqtt_client,
        stop_signal_tx,
        app_err_tx,
        join_handle,
    };

    Ok(Box::new(mqtt_client))
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: MqttClientConf = serde_json::from_value(conf.clone())?;
    match conf.version {
        types::apps::mqtt_client::Version::V311 => match &conf.v311 {
            Some(conf) => {
                if let Some(last_will) = &conf.last_will {
                    BASE64_STANDARD.decode(&last_will.message).map_err(|e| {
                        HaliaError::Common(format!("遗嘱信息base64解码错误: {}", e))
                    })?;
                }
            }
            None => {
                return Err(HaliaError::Common(
                    "配置错误,未填写mqtt v311的配置!".to_string(),
                ))
            }
        },
        types::apps::mqtt_client::Version::V50 => match &conf.v50 {
            Some(conf) => {
                if let Some(last_will) = &conf.last_will {
                    BASE64_STANDARD.decode(&last_will.message).map_err(|e| {
                        HaliaError::Common(format!("遗嘱信息base64解码错误: {}", e))
                    })?;
                }
            }
            None => {
                return Err(HaliaError::Common(
                    "配置错误,未填写mqtt v5.0的配置!".to_string(),
                ))
            }
        },
    }

    Ok(())
}

impl MqttClient {
    fn start_v311(
        conf: MqttClientV311Conf,
        sources: Arc<DashMap<String, Source>>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        app_err_tx: broadcast::Sender<bool>,
        device_err: Arc<RwLock<Option<String>>>,
    ) -> (
        HaliaMqttClient,
        Option<
            JoinHandle<(
                mpsc::Receiver<()>,
                broadcast::Sender<bool>,
                Arc<DashMap<String, Source>>,
            )>,
        >,
    ) {
        let mut mqtt_options = MqttOptions::new(&conf.client_id, &conf.host, conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(conf.keep_alive));
        mqtt_options.set_clean_session(conf.clean_session);
        if let Some(auth) = &conf.auth {
            mqtt_options.set_credentials(auth.username.clone(), auth.password.clone());
        }
        if let Some(cert_info) = &conf.cert_info {
            let transport = Transport::Tls(TlsConfiguration::Simple {
                ca: cert_info.ca_cert.clone().into_bytes(),
                alpn: None,
                client_auth: Some((
                    cert_info.client_cert.clone().into_bytes(),
                    cert_info.client_key.clone().into_bytes(),
                )),
            });
            mqtt_options.set_transport(transport);
        }
        if let Some(last_will) = &conf.last_will {
            let message = BASE64_STANDARD.decode(&last_will.message).unwrap();
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
                        Self::handle_v311_event(event, &sources, &device_err, &mut err, &app_err_tx).await;
                    }
                }
            }
        });

        (HaliaMqttClient::V311(Arc::new(client)), Some(join_handle))
    }

    async fn handle_v311_event(
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

    // async fn start_v50(
    //     &mut self,
    //     sources: Arc<DashMap<String, Source>>,
    //     mut stop_signal_rx: mpsc::Receiver<()>,
    //     app_err_tx: broadcast::Sender<bool>,
    // ) {
    //     let conf = self.conf.v50.as_ref().unwrap();
    //     let mut mqtt_options = v5::MqttOptions::new(&conf.client_id, &conf.host, conf.port);
    //     mqtt_options.set_keep_alive(Duration::from_secs(conf.keep_alive));

    //     if let Some(auth) = &conf.auth {
    //         mqtt_options.set_credentials(&auth.username, &auth.password);
    //     }

    //     if let Some(cert_info) = &conf.cert_info {
    //         let transport = Transport::Tls(TlsConfiguration::Simple {
    //             ca: cert_info.ca_cert.clone().into_bytes(),
    //             alpn: None,
    //             client_auth: Some((
    //                 cert_info.client_cert.clone().into_bytes(),
    //                 cert_info.client_key.clone().into_bytes(),
    //             )),
    //         });
    //         mqtt_options.set_transport(transport);
    //     }

    //     let (client, mut event_loop) = v5::AsyncClient::new(mqtt_options, 16);
    //     let sources = self.sources.clone();

    //     let arc_client = Arc::new(client);
    //     self.halia_mqtt_client = HaliaMqttClient::V50(arc_client);

    //     // let (tx, mut rx) = mpsc::channel(1);
    //     // self.stop_signal_tx = Some(tx);

    //     tokio::spawn(async move {
    //         loop {
    //             select! {
    //                 _ = stop_signal_rx.recv() => {
    //                     return
    //                 }

    //                 event = event_loop.poll() => {
    //                     Self::handle_v50_event(event, &sources).await;
    //                 }
    //             }
    //         }
    //     });
    // }

    async fn handle_v50_event(
        event: Result<v5::Event, rumqttc::v5::ConnectionError>,
        sources: &Arc<DashMap<String, Source>>,
    ) {
        match event {
            Ok(v5::Event::Incoming(v5::Incoming::Publish(p))) => {
                match MessageBatch::from_json(p.payload) {
                    Ok(msg) => {
                        if p.topic.len() > 0 {
                            match String::from_utf8(p.topic.into()) {
                                Ok(topic) => {
                                    for source in sources.iter_mut() {
                                        if matches(&source.conf.topic, &topic) {
                                            if source.mb_tx.receiver_count() > 0 {
                                                source.mb_tx.send(msg.clone());
                                            }
                                        }
                                    }
                                }
                                Err(e) => warn!("{}", e),
                            }
                        } else {
                            match p.properties {
                                Some(properties) => match properties.topic_alias {
                                    Some(msg_topic_alias) => {
                                        for source in sources.iter_mut() {
                                            match source.conf.topic_alias {
                                                Some(source_topic_alias) => {
                                                    if source_topic_alias == msg_topic_alias {
                                                        _ = source.mb_tx.send(msg.clone());
                                                    }
                                                }
                                                None => {}
                                            }
                                        }
                                    }
                                    None => {}
                                },
                                None => {}
                            }
                        }
                    }
                    Err(e) => error!("Failed to decode msg:{}", e),
                }
            }
            Ok(_) => (),
            Err(e) => match e {
                v5::ConnectionError::MqttState(_) => todo!(),
                v5::ConnectionError::Timeout(_) => todo!(),
                v5::ConnectionError::Tls(_) => todo!(),
                v5::ConnectionError::Io(_) => todo!(),
                v5::ConnectionError::ConnectionRefused(_) => todo!(),
                v5::ConnectionError::NotConnAck(_) => todo!(),
                v5::ConnectionError::RequestsDone => todo!(),
            },
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

pub(crate) fn qos_to_v311(qos: &Qos) -> mqttbytes::QoS {
    match qos {
        Qos::AtMostOnce => QoS::AtMostOnce,
        Qos::AtLeastOnce => QoS::AtLeastOnce,
        Qos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

pub(crate) fn qos_to_v50(qos: &Qos) -> v5::mqttbytes::QoS {
    match qos {
        Qos::AtMostOnce => v5::mqttbytes::QoS::AtMostOnce,
        Qos::AtLeastOnce => v5::mqttbytes::QoS::AtLeastOnce,
        Qos::ExactlyOnce => v5::mqttbytes::QoS::ExactlyOnce,
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

        let (stop_signal_rx, app_err_tx, sources) = self.join_handle.take().unwrap().await.unwrap();
        let new_conf: MqttClientConf = serde_json::from_value(new_conf)?;
        let (halia_mqtt_client, join_hanlde) = match new_conf.version {
            types::apps::mqtt_client::Version::V311 => MqttClient::start_v311(
                new_conf.v311.unwrap(),
                sources,
                stop_signal_rx,
                app_err_tx,
                self.err.clone(),
            ),
            types::apps::mqtt_client::Version::V50 => todo!(),
        };
        self.halia_mqtt_client = halia_mqtt_client;
        self.join_handle = join_hanlde;

        for mut sink in self.sinks.iter_mut() {
            sink.restart(self.halia_mqtt_client.clone()).await;
        }

        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
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

        Ok(())
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

        match &self.halia_mqtt_client {
            HaliaMqttClient::V311(client_v311) => {
                if let Err(e) = client_v311.unsubscribe(source.conf.topic).await {
                    error!("unsubscribe err:{e}");
                }
            }
            HaliaMqttClient::V50(client_v50) => {
                if let Err(e) = client_v50.unsubscribe(source.conf.topic).await {
                    error!("unsubscribe err:{e}");
                }
            }
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
            None => return Err(HaliaError::NotFound(sink_id)),
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
