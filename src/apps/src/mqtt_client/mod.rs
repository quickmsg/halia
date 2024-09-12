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
};
use tracing::{error, warn};
use types::{
    apps::{
        mqtt_client::{MqttClientConf, Qos, SinkConf, SourceConf},
        AppConf, AppType, CreateUpdateAppReq,
    },
    BaseConf, CreateUpdateSourceOrSinkReq,
};
use uuid::Uuid;

use crate::App;

mod sink;
mod source;

pub struct MqttClient {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: MqttClientConf,

    err: Arc<RwLock<Option<String>>>,
    stop_signal_tx: mpsc::Sender<()>,
    app_err_tx: broadcast::Sender<bool>,

    sources: Arc<DashMap<Uuid, Source>>,
    sinks: DashMap<Uuid, Sink>,
    halia_mqtt_client: HaliaMqttClient,
}

#[derive(Clone)]
pub(crate) enum HaliaMqttClient {
    V311(Arc<AsyncClient>),
    V50(Arc<v5::AsyncClient>),
}

pub fn new(app_id: Uuid, app_conf: AppConf) -> HaliaResult<Box<dyn App>> {
    let ext_conf: MqttClientConf = serde_json::from_value(app_conf.ext)?;
    // MqttClient::validate_conf(&ext_conf)?;

    let (app_err_tx, _) = broadcast::channel(16);

    let (stop_signal_tx, mut rx) = mpsc::channel(1);

    Ok(Box::new(MqttClient {
        id: app_id,
        base_conf: app_conf.base,
        ext_conf,
        err: Arc::new(RwLock::new(None)),
        sources: Arc::new(DashMap::new()),
        sinks: DashMap::new(),
        halia_mqtt_client: todo!(),
        stop_signal_tx,
        app_err_tx,
    }))
}

impl MqttClient {
    fn validate_conf(conf: &MqttClientConf) -> HaliaResult<()> {
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

    async fn start_v311(&mut self) {
        let conf = self.ext_conf.v311.as_ref().unwrap();

        let mut mqtt_options = MqttOptions::new(&conf.client_id, &conf.host, conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(conf.keep_alive));
        mqtt_options.set_clean_session(conf.clean_session);
        if let Some(auth) = &self.ext_conf.v311.as_ref().unwrap().auth {
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

        let sources = self.sources.clone();
        for source in sources.iter_mut() {
            source.start();
            let _ = client
                .subscribe(
                    source.ext_conf.topic.clone(),
                    qos_to_v311(&source.ext_conf.qos),
                )
                .await;
        }

        let arc_client = Arc::new(client);
        for sink in self.sinks.iter_mut() {
            sink.start_v311(arc_client.clone(), app_err_tx.subscribe());
        }
        self.halia_mqtt_client = HaliaMqttClient::V311(arc_client);

        let err = self.err.clone();
        // let device_err_tx = self.device_err_tx.as_ref().unwrap().clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = rx.recv() => {
                        return
                    }

                    event = event_loop.poll() => {
                        Self::handle_v311_event(event, &sources, &err, &app_err_tx).await;
                    }
                }
            }
        });
    }

    async fn handle_v311_event(
        event: Result<Event, rumqttc::ConnectionError>,
        sources: &Arc<RwLock<Vec<Source>>>,
        err: &Arc<RwLock<Option<String>>>,
        app_err_tx: &broadcast::Sender<bool>,
    ) {
        match event {
            Ok(Event::Incoming(Incoming::Publish(p))) => match MessageBatch::from_json(p.payload) {
                Ok(msg) => {
                    for source in sources.write().await.iter_mut() {
                        if matches(&source.ext_conf.topic, &p.topic) {
                            match &source.mb_tx {
                                Some(tx) => {
                                    if let Err(e) = tx.send(msg.clone()) {
                                        warn!("{}", e);
                                    }
                                }
                                None => {}
                            }
                        }
                    }
                }
                Err(e) => error!("Failed to decode msg:{}", e),
            },
            Ok(_) => {
                _ = app_err_tx.send(true);
                *err.write().await = None;
            }
            Err(e) => {
                _ = app_err_tx.send(false);
                *err.write().await = Some(e.to_string());
            }
        }
    }

    async fn start_v50(&mut self) {
        let conf = self.ext_conf.v50.as_ref().unwrap();
        let mut mqtt_options = v5::MqttOptions::new(&conf.client_id, &conf.host, conf.port);
        mqtt_options.set_keep_alive(Duration::from_secs(conf.keep_alive));

        if let Some(auth) = &conf.auth {
            mqtt_options.set_credentials(&auth.username, &auth.password);
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

        let (client, mut event_loop) = v5::AsyncClient::new(mqtt_options, 16);
        let sources = self.sources.clone();
        for source in sources.write().await.iter_mut() {
            let (mb_tx, _) = broadcast::channel(16);
            source.mb_tx = Some(mb_tx);
            let _ = client
                .subscribe(
                    source.ext_conf.topic.clone(),
                    qos_to_v50(&source.ext_conf.qos),
                )
                .await;
        }

        let arc_client = Arc::new(client);
        for sink in self.sinks.iter_mut() {
            sink.start_v50(
                arc_client.clone(),
                self.app_err_tx.as_ref().unwrap().subscribe(),
            );
        }
        self.client_v50 = Some(arc_client);

        let (tx, mut rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(tx);

        tokio::spawn(async move {
            loop {
                select! {
                    _ = rx.recv() => {
                        return
                    }

                    event = event_loop.poll() => {
                        Self::handle_v50_event(event, &sources).await;
                    }
                }
            }
        });
    }

    async fn handle_v50_event(
        event: Result<v5::Event, rumqttc::v5::ConnectionError>,
        sources: &Arc<RwLock<Vec<Source>>>,
    ) {
        match event {
            Ok(v5::Event::Incoming(v5::Incoming::Publish(p))) => {
                match MessageBatch::from_json(p.payload) {
                    Ok(msg) => {
                        if p.topic.len() > 0 {
                            match String::from_utf8(p.topic.into()) {
                                Ok(topic) => {
                                    for source in sources.write().await.iter_mut() {
                                        if matches(&source.ext_conf.topic, &topic) {
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
                                        for source in sources.write().await.iter_mut() {
                                            match source.ext_conf.topic_alias {
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

fn qos_to_v311(qos: &Qos) -> mqttbytes::QoS {
    match qos {
        Qos::AtMostOnce => QoS::AtMostOnce,
        Qos::AtLeastOnce => QoS::AtLeastOnce,
        Qos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

fn qos_to_v50(qos: &Qos) -> v5::mqttbytes::QoS {
    match qos {
        Qos::AtMostOnce => v5::mqttbytes::QoS::AtMostOnce,
        Qos::AtLeastOnce => v5::mqttbytes::QoS::AtLeastOnce,
        Qos::ExactlyOnce => v5::mqttbytes::QoS::ExactlyOnce,
    }
}

#[async_trait]
impl App for MqttClient {
    fn check_duplicate(&self, req: &CreateUpdateAppReq) -> HaliaResult<()> {
        if self.base_conf.name == req.conf.base.name {
            return Err(HaliaError::NameExists);
        }

        if req.app_type == AppType::MqttClient {
            // TODO
        }

        Ok(())
    }

    async fn update(&mut self, app_conf: AppConf) -> HaliaResult<()> {
        let ext_conf = serde_json::from_value(app_conf.ext)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = app_conf.base;
        self.ext_conf = ext_conf;

        if restart {
            self.stop_signal_tx.send(()).await.unwrap();

            // self.start().await.unwrap();
            for sink in self.sinks.iter_mut() {
                sink.restart(self.halia_mqtt_client.clone()).await;
            }
        }

        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
        self.stop_signal_tx.send(()).await.unwrap();

        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;
        Source::validate_conf(&ext_conf)?;

        // for source in self.sources.read().await.iter() {
        //     source.check_duplicate(&req.base, &ext_conf)?;
        // }

        let mut source = Source::new(source_id, req.base, ext_conf);
        source.start();
        match self.client {
            Client::V311(client_v311) => {
                if let Err(e) = self
                    .client_v311
                    .as_ref()
                    .unwrap()
                    .subscribe(
                        source.ext_conf.topic.clone(),
                        qos_to_v311(&source.ext_conf.qos),
                    )
                    .await
                {
                    error!("client subscribe err:{e}");
                }
            }
            Client::V50(client_v50) => {
                if let Err(e) = self
                    .client_v50
                    .as_ref()
                    .unwrap()
                    .subscribe(
                        source.ext_conf.topic.clone(),
                        qos_to_v50(&source.ext_conf.qos),
                    )
                    .await
                {
                    error!("client subscribe err:{e}");
                }
            }
        }

        // self.sources.(source);

        Ok(())
    }

    async fn update_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;
        Source::validate_conf(&ext_conf)?;

        // for source in self.sources.iter() {
        //     if source.id != source_id {
        //         source.check_duplicate(&req.base, &ext_conf)?;
        //     }
        // }

        let mut source = self
            .sources
            .get_mut(&source_id)
            .ok_or(HaliaError::NotFound)?;

        let restart = source.update(req.base, ext_conf);
        if restart {
            match &self.halia_mqtt_client {
                HaliaMqttClient::V311(client_v311) => {
                    if let Err(e) = client_v311.unsubscribe(source.ext_conf.topic.clone()).await {
                        error!("unsubscribe err:{e}");
                    }
                    if let Err(e) = client_v311
                        .subscribe(
                            source.ext_conf.topic.clone(),
                            qos_to_v311(&source.ext_conf.qos),
                        )
                        .await
                    {
                        error!("subscribe err:{e}");
                    }
                }
                HaliaMqttClient::V50(client_v50) => {
                    if let Err(e) = client_v50.unsubscribe(source.ext_conf.topic.clone()).await {
                        error!("unsubscribe err:{e}");
                    }

                    if let Err(e) = client_v50
                        .subscribe(
                            source.ext_conf.topic.clone(),
                            qos_to_v50(&source.ext_conf.qos),
                        )
                        .await
                    {
                        error!("subscribe err:{e}");
                    }
                }
            }
        }

        Ok(())
    }

    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()> {
        // todo unsubscribe topic
        // match &self.client {
        //     Client::V311(client_v311) => {
        //         if let Err(e) = client_v311.unsubscribe(source.ext_conf.topic.clone()).await {
        //             error!("unsubscribe err:{e}");
        //         }
        //         if let Err(e) = client_v311
        //             .subscribe(
        //                 source.ext_conf.topic.clone(),
        //                 qos_to_v311(&source.ext_conf.qos),
        //             )
        //             .await
        //         {
        //             error!("subscribe err:{e}");
        //         }
        //     }
        //     Client::V50(client_v50) => {
        //         if let Err(e) = client_v50.unsubscribe(source.ext_conf.topic.clone()).await {
        //             error!("unsubscribe err:{e}");
        //         }

        //         if let Err(e) = client_v50
        //             .subscribe(
        //                 source.ext_conf.topic.clone(),
        //                 qos_to_v50(&source.ext_conf.qos),
        //             )
        //             .await
        //         {
        //             error!("subscribe err:{e}");
        //         }
        //     }
        // }
        self.sources.remove(&source_id);
        Ok(())
    }

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        for sink in self.sinks.iter() {
            // sink.check_duplicate(&req.base, &ext_conf)?;
        }

        let mut sink = Sink::new(sink_id, req.base, ext_conf).await?;
        sink.start(self.halia_mqtt_client.clone(), self.app_err_tx.subscribe());
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        for sink in self.sinks.iter() {
            if sink.id != sink_id {
                sink.check_duplicate(&req.base, &ext_conf)?;
            }
        }

        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => sink.update(req.base, ext_conf).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => sink.stop().await,
            None => unreachable!(),
        }
        self.sinks.remove(&sink_id);
        Ok(())
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        Ok(self
            .sources
            .get(&source_id)
            .ok_or(HaliaError::NotFound)?
            .mb_tx
            .subscribe())
    }

    async fn get_sink_tx(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        Ok(self
            .sinks
            .get(&sink_id)
            .ok_or(HaliaError::NotFound)?
            .mb_tx
            .clone())
    }
}
