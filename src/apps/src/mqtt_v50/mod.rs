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
use tracing::{error, warn};
use message::{MessageBatch, RuleMessageBatch};
use rumqttc::v5::{
    self,
    mqttbytes::v5::{ConnectProperties, LastWill},
    AsyncClient,
};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{
        broadcast,
        mpsc::{self, UnboundedSender},
        watch, RwLock,
    },
    task::JoinHandle,
};
use tracing::debug;
use types::apps::{
    mqtt_client_v50::{MqttClientConf, Qos, SinkConf, SourceConf},
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

    sources: Arc<DashMap<String, Source>>,
    sinks: DashMap<String, Sink>,
    mqtt_client: Arc<AsyncClient>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    rtt: AtomicU16,
}

struct JoinHandleData {
    conf: MqttClientConf,
    stop_signal_rx: watch::Receiver<()>,
    app_err_tx: broadcast::Sender<bool>,
    sources: Arc<DashMap<String, Source>>,
    app_err: Arc<RwLock<Option<String>>>,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: MqttClientConf = serde_json::from_value(conf).unwrap();
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
        mut join_handle_data: JoinHandleData,
    ) -> (JoinHandle<JoinHandleData>, Arc<AsyncClient>) {
        let mut mqtt_options = v5::MqttOptions::new(
            &join_handle_data.conf.client_id,
            &join_handle_data.conf.host,
            join_handle_data.conf.port,
        );
        mqtt_options.set_keep_alive(Duration::from_secs(join_handle_data.conf.keep_alive));

        mqtt_options.set_clean_start(join_handle_data.conf.clean_start);

        match join_handle_data.conf.auth_method {
            types::apps::mqtt_client_v50::AuthMethod::None => {}
            types::apps::mqtt_client_v50::AuthMethod::Password => {
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

        mqtt_options.set_clean_start(join_handle_data.conf.clean_start);

        if join_handle_data.conf.connect_properties_enable {
            if let Some(conf_connect_properties) = &join_handle_data.conf.connect_properties {
                let mut connect_properties = ConnectProperties::new();
                connect_properties.session_expiry_interval =
                    conf_connect_properties.session_expire_interval;
                connect_properties.receive_maximum = conf_connect_properties.receive_maximum;
                connect_properties.max_packet_size = conf_connect_properties.max_packet_size;
                connect_properties.topic_alias_max = conf_connect_properties.topic_alias_max;
                connect_properties.request_response_info =
                    conf_connect_properties.request_response_info;
                connect_properties.request_problem_info =
                    conf_connect_properties.request_problem_info;
                connect_properties.user_properties =
                    conf_connect_properties.user_properties.clone();
                connect_properties.authentication_method =
                    conf_connect_properties.authentication_method.clone();
                // TODO
                // connect_properties.authentication_data = conf_connect_properties.authentication_data;

                mqtt_options.set_connect_properties(connect_properties);
            }
        }

        if join_handle_data.conf.last_will_enable {
            let last_will = join_handle_data.conf.last_will.as_ref().unwrap();
            mqtt_options.set_last_will(LastWill {
                topic: todo!(),
                message: todo!(),
                qos: todo!(),
                retain: todo!(),
                properties: todo!(),
            });
        }

        if join_handle_data.conf.ssl_enable {
            let config = get_ssl_config(&join_handle_data.conf.ssl_conf.as_ref().unwrap());
            let transport =
                rumqttc::Transport::Tls(rumqttc::TlsConfiguration::Rustls(Arc::new(config)));
            mqtt_options.set_transport(transport);
        }

        // connect_properties.authentication_method = conf.authentication_method;
        // if let Some(authehtication_data) = &conf.authentication_data {
        //     match authehtication_data.typ {
        //         types::PlainOrBase64ValueType::Plain => {
        //             connect_properties.authentication_data = Some(
        //                 serde_json::to_vec(&authehtication_data.value)
        //                     .unwrap()
        //                     .into(),
        //             );
        //         }
        //         types::PlainOrBase64ValueType::Base64 => {
        //             let b = BASE64_STANDARD
        //                 .decode(authehtication_data.value.as_str().unwrap())
        //                 .unwrap();
        //             connect_properties.authentication_data = Some(b.into());
        //         }
        //     }
        // }

        let (client, mut event_loop) = v5::AsyncClient::new(mqtt_options, 16);

        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    event = event_loop.poll() => {
                        Self::handle_event(event, &join_handle_data.sources).await;
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
                debug!("Received: {:?}", p);
                match MessageBatch::from_json(p.payload) {
                    Ok(msg) => {
                        if p.topic.len() > 0 {
                            match String::from_utf8(p.topic.into()) {
                                Ok(topic) => {
                                    for mut source in sources.iter_mut() {
                                        if matches(&source.conf.topic, &topic) {
                                            match source.mb_txs.len() {
                                                0 => {}
                                                1 => {
                                                    let mb = RuleMessageBatch::Owned(msg.clone());
                                                    if let Err(e) = source.mb_txs[0].send(mb) {
                                                        warn!("{}", e);
                                                        source.mb_txs.remove(0);
                                                    }
                                                }
                                                _ => {
                                                    let rmb = RuleMessageBatch::Arc(Arc::new(
                                                        msg.clone(),
                                                    ));
                                                    source
                                                        .mb_txs
                                                        .retain(|tx| tx.send(rmb.clone()).is_ok());
                                                }
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
                                        for mut source in sources.iter_mut() {
                                            match source.conf.topic_alias {
                                                Some(source_topic_alias) => {
                                                    if source_topic_alias == msg_topic_alias {
                                                        match source.mb_txs.len() {
                                                            0 => {}
                                                            1 => {
                                                                let mb = RuleMessageBatch::Owned(
                                                                    msg.clone(),
                                                                );
                                                                if let Err(e) =
                                                                    source.mb_txs[0].send(mb)
                                                                {
                                                                    warn!("{}", e);
                                                                    source.mb_txs.remove(0);
                                                                }
                                                            }
                                                            _ => {
                                                                let rmb = RuleMessageBatch::Arc(
                                                                    Arc::new(msg.clone()),
                                                                );
                                                                source.mb_txs.retain(|tx| {
                                                                    tx.send(rmb.clone()).is_ok()
                                                                });
                                                            }
                                                        }
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
        let new_conf: MqttClientConf = serde_json::from_value(new_conf)?;

        self.stop_signal_tx.send(()).unwrap();

        let mut join_handle_data = self.join_handle.take().unwrap().await.unwrap();
        join_handle_data.conf = new_conf;

        let (join_hanlde, mqtt_client) = MqttClient::event_loop(join_handle_data);
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
        self.stop_signal_tx.send(()).unwrap();
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

    async fn get_source_rxs(
        &self,
        source_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedReceiver<RuleMessageBatch>>> {
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
