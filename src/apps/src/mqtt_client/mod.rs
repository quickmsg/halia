use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common::{
    active_sink_ref, active_source_ref, check_and_set_on_false, check_and_set_on_true,
    check_delete, check_delete_item, check_stop, deactive_sink_ref, deactive_source_ref,
    del_sink_ref, del_source_ref,
    error::{HaliaError, HaliaResult},
    find_sink_add_ref, find_source_add_ref,
    ref_info::RefInfo,
};
use message::MessageBatch;
use rumqttc::{
    mqttbytes, v5, AsyncClient, Event, Incoming, MqttOptions, QoS, TlsConfiguration, Transport,
};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
};
use tracing::{debug, error, warn};
use types::{
    apps::{
        mqtt_client::{MqttClientConf, Qos, SinkConf, SourceConf},
        AppConf, AppType, CreateUpdateAppReq, QueryParams, SearchAppsItemConf, SearchAppsItemResp,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksItemResp,
    SearchSourcesOrSinksResp,
};
use uuid::Uuid;

use crate::{sink_not_found_err, source_not_found_err, App};

mod sink;
mod source;

pub struct MqttClient {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: MqttClientConf,

    on: bool,
    err: Arc<RwLock<Option<String>>>,
    stop_signal_tx: Option<mpsc::Sender<()>>,

    sources: Arc<RwLock<Vec<Source>>>,
    sources_ref_infos: Vec<(Uuid, RefInfo)>,
    sinks: Vec<Sink>,
    sinks_ref_infos: Vec<(Uuid, RefInfo)>,
    client_v311: Option<Arc<AsyncClient>>,
    client_v50: Option<Arc<v5::AsyncClient>>,
}

pub fn new(app_id: Uuid, app_conf: AppConf) -> HaliaResult<Box<dyn App>> {
    let ext_conf: MqttClientConf = serde_json::from_value(app_conf.ext)?;
    MqttClient::validate_conf(&ext_conf)?;

    Ok(Box::new(MqttClient {
        id: app_id,
        base_conf: app_conf.base,
        ext_conf,
        on: false,
        err: Arc::new(RwLock::new(None)),
        sources: Arc::new(RwLock::new(vec![])),
        sources_ref_infos: vec![],
        sinks: vec![],
        sinks_ref_infos: vec![],
        client_v311: None,
        client_v50: None,
        stop_signal_tx: None,
    }))
}

impl MqttClient {
    fn validate_conf(_conf: &MqttClientConf) -> HaliaResult<()> {
        Ok(())
    }

    fn check_on(&self) -> HaliaResult<()> {
        match self.on {
            true => Ok(()),
            false => Err(HaliaError::Stopped(format!(
                "mqtt客户端应用:{}",
                self.base_conf.name
            ))),
        }
    }

    async fn start_v311(&mut self) {
        let mut mqtt_options = MqttOptions::new(
            self.ext_conf.client_id.clone(),
            self.ext_conf.host.clone(),
            self.ext_conf.port,
        );

        mqtt_options.set_keep_alive(Duration::from_secs(self.ext_conf.keep_alive));

        if let Some(auth) = &self.ext_conf.auth {
            mqtt_options.set_credentials(auth.username.clone(), auth.password.clone());
        }

        if let Some(cert_info) = &self.ext_conf.cert_info {
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

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 16);

        let sources = self.sources.clone();
        for source in sources.write().await.iter_mut() {
            let (mb_tx, _) = broadcast::channel(16);
            source.mb_tx = Some(mb_tx);
            let _ = client
                .subscribe(
                    source.ext_conf.topic.clone(),
                    get_mqtt_v311_qos(&source.ext_conf.qos),
                )
                .await;
        }

        let arc_client = Arc::new(client);
        for sink in self.sinks.iter_mut() {
            sink.start_v311(arc_client.clone());
        }
        self.client_v311 = Some(arc_client);

        let (tx, mut rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(tx);

        let err = self.err.clone();
        tokio::spawn(async move {
            loop {
                select! {
                    _ = rx.recv() => {
                        return
                    }

                    event = event_loop.poll() => {
                        Self::handle_v311_event(event, &sources, &err).await;
                    }
                }
            }
        });
    }

    async fn handle_v311_event(
        event: Result<Event, rumqttc::ConnectionError>,
        sources: &Arc<RwLock<Vec<Source>>>,
        err: &Arc<RwLock<Option<String>>>,
    ) {
        debug!("{:?}", event);
        match event {
            Ok(Event::Incoming(Incoming::Publish(p))) => match MessageBatch::from_json(p.payload) {
                Ok(msg) => {
                    for source in sources.write().await.iter_mut() {
                        if matches(&source.ext_conf.topic, &p.topic) {
                            match &source.mb_tx {
                                Some(tx) => {
                                    if let Err(e) = tx.send(msg.clone()) {
                                        debug!("{}", e);
                                    }
                                }
                                None => {}
                            }
                        }
                    }
                }
                Err(e) => error!("Failed to decode msg:{}", e),
            },
            Ok(_) => *err.write().await = None,
            Err(e) => {
                *err.write().await = Some(e.to_string());
            }
        }
    }

    async fn start_v50(&mut self) {
        let mut mqtt_options = v5::MqttOptions::new(
            self.ext_conf.client_id.clone(),
            self.ext_conf.host.clone(),
            self.ext_conf.port,
        );
        mqtt_options.set_keep_alive(Duration::from_secs(self.ext_conf.keep_alive));

        if let Some(auth) = &self.ext_conf.auth {
            mqtt_options.set_credentials(auth.username.clone(), auth.password.clone());
        }

        if let Some(cert_info) = &self.ext_conf.cert_info {
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
                    get_mqtt_v50_qos(&source.ext_conf.qos),
                )
                .await;
        }

        let arc_client = Arc::new(client);
        for sink in self.sinks.iter_mut() {
            sink.start_v50(arc_client.clone());
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
                                            match &source.mb_tx {
                                                Some(tx) => {
                                                    let _ = tx.send(msg.clone());
                                                }
                                                None => {}
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
                                                        _ = source
                                                            .mb_tx
                                                            .as_ref()
                                                            .unwrap()
                                                            .send(msg.clone());
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

fn get_mqtt_v311_qos(qos: &Qos) -> mqttbytes::QoS {
    match qos {
        Qos::AtMostOnce => QoS::AtMostOnce,
        Qos::AtLeastOnce => QoS::AtLeastOnce,
        Qos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

fn get_mqtt_v50_qos(qos: &Qos) -> v5::mqttbytes::QoS {
    match qos {
        Qos::AtMostOnce => v5::mqttbytes::QoS::AtMostOnce,
        Qos::AtLeastOnce => v5::mqttbytes::QoS::AtLeastOnce,
        Qos::ExactlyOnce => v5::mqttbytes::QoS::ExactlyOnce,
    }
}

#[async_trait]
impl App for MqttClient {
    fn get_id(&self) -> &Uuid {
        &self.id
    }

    fn check_duplicate(&self, req: &CreateUpdateAppReq) -> HaliaResult<()> {
        if self.base_conf.name == req.conf.base.name {
            return Err(HaliaError::NameExists);
        }

        if req.app_type == AppType::MqttClient {
            // TODO
        }

        Ok(())
    }

    async fn search(&self) -> SearchAppsItemResp {
        SearchAppsItemResp {
            id: self.id,
            on: self.on,
            app_type: AppType::MqttClient,
            conf: SearchAppsItemConf {
                base: self.base_conf.clone(),
                ext: serde_json::json!(self.ext_conf),
            },
            err: self.err.read().await.clone(),
            rtt: 1,
        }
    }

    async fn update(&mut self, app_conf: AppConf) -> HaliaResult<()> {
        let ext_conf = serde_json::from_value(app_conf.ext)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = app_conf.base;
        self.ext_conf = ext_conf;

        if self.on && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            self.start().await.unwrap();
            for sink in self.sinks.iter_mut() {
                match self.ext_conf.version {
                    types::apps::mqtt_client::Version::V311 => {
                        sink.restart_v311(self.client_v311.as_ref().unwrap().clone())
                            .await
                    }
                    types::apps::mqtt_client::Version::V50 => {
                        sink.restart_v50(self.client_v50.as_ref().unwrap().clone())
                            .await
                    }
                }
            }
        }

        Ok(())
    }

    async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);

        match self.ext_conf.version {
            types::apps::mqtt_client::Version::V311 => self.start_v311().await,
            types::apps::mqtt_client::Version::V50 => self.start_v50().await,
        }

        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        check_stop!(self, sources_ref_infos);
        check_stop!(self, sinks_ref_infos);

        check_and_set_on_false!(self);

        for sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;
        match self.ext_conf.version {
            types::apps::mqtt_client::Version::V311 => self.client_v311 = None,
            types::apps::mqtt_client::Version::V50 => self.client_v50 = None,
        }

        Ok(())
    }

    async fn delete(&mut self) -> HaliaResult<()> {
        if self.on {
            return Err(HaliaError::Running);
        }

        check_delete!(self, sources_ref_infos);
        check_delete!(self, sinks_ref_infos);

        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;
        Source::validate_conf(&ext_conf)?;

        for source in self.sources.read().await.iter() {
            source.check_duplicate(&req.base, &ext_conf)?;
        }

        let source = Source::new(source_id, req.base, ext_conf);
        if self.on {
            match self.ext_conf.version {
                types::apps::mqtt_client::Version::V311 => {
                    if let Err(e) = self
                        .client_v311
                        .as_ref()
                        .unwrap()
                        .subscribe(
                            source.ext_conf.topic.clone(),
                            get_mqtt_v311_qos(&source.ext_conf.qos),
                        )
                        .await
                    {
                        error!("client subscribe err:{e}");
                    }
                }
                types::apps::mqtt_client::Version::V50 => {
                    if let Err(e) = self
                        .client_v50
                        .as_ref()
                        .unwrap()
                        .subscribe(
                            source.ext_conf.topic.clone(),
                            get_mqtt_v50_qos(&source.ext_conf.qos),
                        )
                        .await
                    {
                        error!("client subscribe err:{e}");
                    }
                }
            }
        }

        self.sources.write().await.push(source);

        Ok(())
    }

    async fn search_sources(
        &self,
        pagination: Pagination,
        query_params: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut total = 0;
        let mut data = vec![];

        for (index, source) in self.sources.read().await.iter().rev().enumerate() {
            let source = source.search();

            if let Some(query_name) = &query_params.name {
                if !source.conf.base.name.contains(query_name) {
                    continue;
                }
            }

            if pagination.check(total) {
                unsafe {
                    data.push(SearchSourcesOrSinksItemResp {
                        info: source,
                        rule_ref: self.sinks_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    })
                }
            }

            total += 1;
        }

        SearchSourcesOrSinksResp { total, data }
    }

    async fn update_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;
        Source::validate_conf(&ext_conf)?;

        for source in self.sources.read().await.iter() {
            if source.id != source_id {
                source.check_duplicate(&req.base, &ext_conf)?;
            }
        }

        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => {
                let restart = source.update(req.base, ext_conf);
                if self.on && restart {
                    match self.ext_conf.version {
                        types::apps::mqtt_client::Version::V311 => {
                            if let Err(e) = self
                                .client_v311
                                .as_ref()
                                .unwrap()
                                .unsubscribe(source.ext_conf.topic.clone())
                                .await
                            {
                                error!("unsubscribe err:{e}");
                            }

                            if let Err(e) = self
                                .client_v311
                                .as_ref()
                                .unwrap()
                                .subscribe(
                                    source.ext_conf.topic.clone(),
                                    get_mqtt_v311_qos(&source.ext_conf.qos),
                                )
                                .await
                            {
                                error!("subscribe err:{e}");
                            }
                        }
                        types::apps::mqtt_client::Version::V50 => {
                            if let Err(e) = self
                                .client_v50
                                .as_ref()
                                .unwrap()
                                .unsubscribe(source.ext_conf.topic.clone())
                                .await
                            {
                                error!("unsubscribe err:{e}");
                            }

                            if let Err(e) = self
                                .client_v50
                                .as_ref()
                                .unwrap()
                                .subscribe(
                                    source.ext_conf.topic.clone(),
                                    get_mqtt_v50_qos(&source.ext_conf.qos),
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
            None => source_not_found_err!(),
        }
    }

    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()> {
        check_delete_item!(self, sources_ref_infos, source_id);
        self.sources
            .write()
            .await
            .retain(|source| source.id != source_id);
        self.sources_ref_infos.retain(|(id, _)| *id != source_id);
        Ok(())
    }

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        for sink in self.sinks.iter() {
            sink.check_duplicate(&req.base, &ext_conf)?;
        }

        let sink = Sink::new(sink_id, req.base, ext_conf).await?;
        self.sinks.push(sink);
        Ok(())
    }

    async fn search_sinks(
        &self,
        pagination: Pagination,
        query_params: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut total = 0;
        let mut data = vec![];
        for (index, sink) in self.sinks.iter().rev().enumerate() {
            let sink = sink.search();
            if let Some(query_name) = &query_params.name {
                if !sink.conf.base.name.contains(query_name) {
                    continue;
                }
            }

            if pagination.check(total) {
                unsafe {
                    data.push(SearchSourcesOrSinksItemResp {
                        info: sink,
                        rule_ref: self.sinks_ref_infos.get_unchecked(index).1.get_rule_ref(),
                    })
                }
            }

            total += 1;
        }
        SearchSourcesOrSinksResp { total, data }
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
            None => sink_not_found_err!(),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        check_delete_item!(self, sinks_ref_infos, sink_id);
        if self.on {
            match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
                Some(sink) => sink.stop().await,
                None => unreachable!(),
            }
        }
        self.sinks.retain(|sink| sink.id != sink_id);
        self.sinks_ref_infos.retain(|(id, _)| *id != sink_id);
        Ok(())
    }

    async fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        find_source_add_ref!(self, source_id, rule_id)
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        self.check_on()?;
        active_source_ref!(self, source_id, rule_id);
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.mb_tx.as_ref().unwrap().subscribe()),
            None => source_not_found_err!(),
        }
    }

    async fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        deactive_source_ref!(self, source_id, rule_id)
    }

    async fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_source_ref!(self, source_id, rule_id)
    }

    async fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        find_sink_add_ref!(self, sink_id, rule_id)
    }

    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        self.check_on()?;
        active_sink_ref!(self, sink_id, rule_id);
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.mb_tx.as_ref().unwrap().clone()),
            None => unreachable!(),
        }
    }

    async fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        deactive_sink_ref!(self, sink_id, rule_id)
    }

    async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        del_sink_ref!(self, sink_id, rule_id)
    }
}
