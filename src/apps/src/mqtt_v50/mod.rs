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
use log::{error, warn};
use message::{MessageBatch, RuleMessageBatch};
use rumqttc::v5::{self, mqttbytes::v5::ConnectProperties, AsyncClient};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{self, UnboundedSender},
        RwLock,
    },
    task::JoinHandle,
};
use types::apps::{
    mqtt_client_v50::{MqttClientConf, Qos, SinkConf, SourceConf},
    SearchAppsItemRunningInfo,
};

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
    mqtt_client: Arc<AsyncClient>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            broadcast::Sender<bool>,
            Arc<DashMap<String, Source>>,
        )>,
    >,
    rtt: AtomicU16,
}

// struct JoinHandleData {
//     conf: MqttClientConf,
//     stop_signal_rx: mpsc::Receiver<()>,
//     app_err_tx: broadcast::Sender<bool>,
//     sources: Arc<DashMap<String, Source>>,
//     app_err: Arc<RwLock<Option<String>>>,
// }

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: MqttClientConf = serde_json::from_value(conf).unwrap();

    let (app_err_tx, _) = broadcast::channel(16);

    let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);

    let sources = Arc::new(DashMap::new());
    let app_err = Arc::new(RwLock::new(None));
    let (join_handle, mqtt_client) = MqttClient::event_loop(
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
        mqtt_client,
        stop_signal_tx,
        app_err_tx,
        join_handle: Some(join_handle),
        rtt: AtomicU16::new(0),
    })
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: MqttClientConf = serde_json::from_value(conf.clone())?;
    if let Some(last_will) = &conf.last_will {
        BASE64_STANDARD
            .decode(&last_will.message)
            .map_err(|e| HaliaError::Common(format!("遗嘱信息base64解码错误: {}", e)))?;
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
    fn event_loop(
        conf: MqttClientConf,
        sources: Arc<DashMap<String, Source>>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        app_err_tx: broadcast::Sender<bool>,
        _device_err: Arc<RwLock<Option<String>>>,
    ) -> (
        JoinHandle<(
            mpsc::Receiver<()>,
            broadcast::Sender<bool>,
            Arc<DashMap<String, Source>>,
        )>,
        Arc<AsyncClient>,
    ) {
        let mut mqtt_options = v5::MqttOptions::new(&conf.client_id, &conf.host, conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(conf.keep_alive));

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

        mqtt_options.set_clean_start(conf.clean_start);
        let mut connect_properties = ConnectProperties::new();
        connect_properties.session_expiry_interval = conf.session_expire_interval;
        connect_properties.receive_maximum = conf.receive_maximum;
        connect_properties.max_packet_size = conf.max_packet_size;
        connect_properties.topic_alias_max = conf.topic_alias_max;
        connect_properties.request_response_info = conf.request_response_info;
        connect_properties.request_problem_info = conf.request_problem_info;
        if let Some(user_properties) = &conf.user_properties {
            for (k, v) in user_properties {
                connect_properties
                    .user_properties
                    .push((k.clone(), v.clone()));
            }
        }
        connect_properties.authentication_method = conf.authentication_method;
        if let Some(authehtication_data) = &conf.authentication_data {
            match authehtication_data.typ {
                types::PlainOrBase64ValueType::Plain => {
                    connect_properties.authentication_data = Some(
                        serde_json::to_vec(&authehtication_data.value)
                            .unwrap()
                            .into(),
                    );
                }
                types::PlainOrBase64ValueType::Base64 => {
                    let b = BASE64_STANDARD
                        .decode(authehtication_data.value.as_str().unwrap())
                        .unwrap();
                    connect_properties.authentication_data = Some(b.into());
                }
            }
        }
        mqtt_options.set_connect_properties(connect_properties);

        // match (conf.ssl.enable, conf.ssl.client_cert_enable) {
        //     (true, true) => {
        //         let transport = Transport::Tls(TlsConfiguration::Simple {
        //             ca: conf.ssl.ca_cert.unwrap().clone().into_bytes(),
        //             alpn: None,
        //             client_auth: Some((
        //                 conf.ssl.client_cert.unwrap().clone().into_bytes(),
        //                 conf.ssl.client_key.unwrap().clone().into_bytes(),
        //             )),
        //         });
        //         mqtt_options.set_transport(transport);
        //     }
        //     (true, false) => {
        //         let transport = Transport::Tls(TlsConfiguration::Simple {
        //             ca: conf.ssl.ca_cert.unwrap().clone().into_bytes(),
        //             alpn: None,
        //             client_auth: None,
        //         });
        //         mqtt_options.set_transport(transport);
        //     }
        //     _ => {}
        // }

        let (client, mut event_loop) = v5::AsyncClient::new(mqtt_options, 16);

        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, app_err_tx, sources)
                    }

                    event = event_loop.poll() => {
                        Self::handle_event(event, &sources).await;
                    }
                }
            }
        });

        (join_handle, Arc::new(client))
    }

    async fn handle_event(
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
                                                source.mb_tx.send(msg.clone()).unwrap();
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

fn transfer_qos(qos: &Qos) -> v5::mqttbytes::QoS {
    match qos {
        Qos::AtMostOnce => v5::mqttbytes::QoS::AtMostOnce,
        Qos::AtLeastOnce => v5::mqttbytes::QoS::AtLeastOnce,
        Qos::ExactlyOnce => v5::mqttbytes::QoS::ExactlyOnce,
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
        self.stop_signal_tx.send(()).await.unwrap();

        let (stop_signal_rx, app_err_tx, sources) = self.join_handle.take().unwrap().await.unwrap();
        let new_conf: MqttClientConf = serde_json::from_value(new_conf)?;
        let (join_hanlde, mqtt_client) = MqttClient::event_loop(
            new_conf,
            sources,
            stop_signal_rx,
            app_err_tx,
            self.err.clone(),
        );
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

        let _ = self.mqtt_client.disconnect().await;
        self.stop_signal_tx.send(()).await.unwrap();
    }

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;

        let source = Source::new(conf);

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
        _source_id: &String,
    ) -> HaliaResult<mpsc::UnboundedReceiver<RuleMessageBatch>> {
        todo!()
        // match self.sources.get(source_id) {
        //     Some(source) => Ok(source.mb_tx.subscribe()),
        //     None => Err(HaliaError::NotFound(source_id.to_owned())),
        // }
    }

    async fn get_sink_tx(
        &self,
        _sink_id: &String,
    ) -> HaliaResult<UnboundedSender<RuleMessageBatch>> {
        todo!()
        // match self.sinks.get(sink_id) {
        //     Some(sink) => Ok(sink.mb_tx.clone()),
        //     None => Err(HaliaError::NotFound(sink_id.to_owned())),
        // }
    }
}
