use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
};
use message::MessageBatch;
use rumqttc::{mqttbytes, v5, AsyncClient, Event, Incoming, MqttOptions, QoS};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tracing::{error, warn};
use types::{
    apps::{
        mqtt_client::{MqttClientConf, Qos},
        AppConf, AppType, QueryParams, SearchAppsItemConf, SearchAppsItemResp,
    },
    BaseConf, CreateUpdateSourceOrSinkReq, Pagination, SearchSourcesOrSinksResp,
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
    sinks: Vec<Sink>,
    client_v311: Option<Arc<AsyncClient>>,
    client_v50: Option<Arc<v5::AsyncClient>>,
}

pub async fn new(app_id: Uuid, app_conf: AppConf) -> HaliaResult<Box<dyn App>> {
    let ext_conf: MqttClientConf = serde_json::from_value(app_conf.ext)?;

    Ok(Box::new(MqttClient {
        id: app_id,
        base_conf: app_conf.base,
        ext_conf,
        on: false,
        err: Arc::new(RwLock::new(None)),
        sources: Arc::new(RwLock::new(vec![])),
        sinks: vec![],
        client_v311: None,
        client_v50: None,
        stop_signal_tx: None,
    }))
}

impl MqttClient {
    async fn start_v311(&mut self) {
        let mut mqtt_options = MqttOptions::new(
            self.ext_conf.client_id.clone(),
            self.ext_conf.host.clone(),
            self.ext_conf.port,
        );

        mqtt_options.set_keep_alive(Duration::from_secs(self.ext_conf.keep_alive));

        match (&self.ext_conf.username, &self.ext_conf.password) {
            (Some(username), Some(password)) => {
                mqtt_options.set_credentials(username.clone(), password.clone());
            }
            (_, _) => {}
        }

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 16);

        let err = self.err.clone();

        if self.ext_conf.ssl {
            match (
                &self.ext_conf.ca,
                &self.ext_conf.client_cert,
                &self.ext_conf.client_key,
            ) {
                (Some(ca), Some(client_cert), Some(client_key)) => {
                    // let mut root_store = RootCertStore::empty();
                    // root_store.add(ca);
                }
                _ => {}
            }
        }

        let sources = self.sources.clone();
        for source in sources.read().await.iter() {
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

        tokio::spawn(async move {
            loop {
                select! {
                    _ = rx.recv() => {
                        return
                    }

                    event = event_loop.poll() => {
                        Self::handle_v311_event(event, &sources).await;
                    }
                }
            }
        });
    }

    async fn handle_v311_event(
        event: Result<Event, rumqttc::ConnectionError>,
        sources: &Arc<RwLock<Vec<Source>>>,
    ) {
        match event {
            Ok(Event::Incoming(Incoming::Publish(p))) => match MessageBatch::from_json(p.payload) {
                Ok(msg) => {
                    for source in sources.write().await.iter_mut() {
                        if matches(&source.ext_conf.topic, &p.topic) {
                            match &source.mb_tx {
                                Some(tx) => {
                                    let _ = tx.send(msg.clone());
                                }
                                None => {}
                            }
                        }
                    }
                }
                Err(e) => error!("Failed to decode msg:{}", e),
            },
            Ok(_) => (),
            Err(e) => {
                match e {
                    rumqttc::ConnectionError::MqttState(e) => {
                        error!("mqtt connection refused:{:?}", e);
                    }
                    rumqttc::ConnectionError::NetworkTimeout => todo!(),
                    rumqttc::ConnectionError::FlushTimeout => todo!(),
                    rumqttc::ConnectionError::Tls(_) => todo!(),
                    rumqttc::ConnectionError::Io(e) => {
                        error!("mqtt connection refused:{:?}", e);
                    }
                    rumqttc::ConnectionError::ConnectionRefused(e) => {
                        error!("mqtt connection refused:{:?}", e);
                    }
                    rumqttc::ConnectionError::NotConnAck(_) => todo!(),
                    rumqttc::ConnectionError::RequestsDone => todo!(),
                }
                time::sleep(10 * Duration::SECOND).await;
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

        if self.ext_conf.username.is_some() && self.ext_conf.password.is_some() {
            mqtt_options.set_credentials(
                self.ext_conf.username.as_ref().unwrap().clone(),
                self.ext_conf.password.as_ref().unwrap().clone(),
            );
        }

        let (client, mut event_loop) = v5::AsyncClient::new(mqtt_options, 16);
        let sources = self.sources.clone();
        for source in sources.read().await.iter() {
            let _ = client
                .subscribe(
                    source.ext_conf.topic.clone(),
                    get_mqtt_v50_qos(&source.ext_conf.qos),
                )
                .await;
        }
        self.client_v50 = Some(Arc::new(client));

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

    async fn search(&self) -> SearchAppsItemResp {
        SearchAppsItemResp {
            id: self.id,
            on: self.on,
            typ: AppType::MqttClient,
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
        check_and_set_on_false!(self);

        if self
            .sources
            .read()
            .await
            .iter()
            .any(|source| source.ref_info.can_stop())
        {
            return Err(HaliaError::Common("有源正在被引用中".to_owned()));
        }

        if self.sinks.iter().any(|sink| sink.ref_info.can_stop()) {
            return Err(HaliaError::Common("有动作正在被引用中".to_owned()));
        }

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

        if self
            .sources
            .read()
            .await
            .iter()
            .any(|source| !source.ref_info.can_delete())
        {
            return Err(HaliaError::Common("有源正在被引用中".to_owned()));
        }

        if self.sinks.iter().any(|sink| !sink.ref_info.can_delete()) {
            return Err(HaliaError::Common("有动作正被规则正在被引用中".to_owned()));
        }

        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        // for source in self.sources.read().await.iter() {
        //     source.check_duplicate(&req)?;
        // }

        match Source::new(source_id, req).await {
            Ok(source) => {
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
            Err(e) => Err(e),
        }
    }

    async fn search_sources(
        &self,
        pagination: Pagination,
        query_params: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut total = 0;
        let mut data = vec![];

        for source in self.sources.read().await.iter().rev() {
            let source = source.search();

            if let Some(query_name) = &query_params.name {
                if !source.conf.base.name.contains(query_name) {
                    continue;
                }
            }

            if total >= (pagination.page - 1) * pagination.size
                && total < pagination.page * pagination.size
            {
                data.push(source);
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
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => match source.update(req).await {
                Ok(restart) => {
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
                Err(e) => Err(e),
            },
            None => source_not_found_err!(),
        }
    }

    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()> {
        match self
            .sources
            .read()
            .await
            .iter()
            .find(|source| source.id == source_id)
        {
            Some(source) => source.delete().await,
            None => source_not_found_err!(),
        }
    }

    async fn create_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(sink_id, req).await {
            Ok(sink) => {
                self.sinks.push(sink);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sinks(
        &self,
        pagination: Pagination,
        query_params: QueryParams,
    ) -> SearchSourcesOrSinksResp {
        let mut total = 0;
        let mut data = vec![];
        for sink in self.sinks.iter().rev() {
            let sink = sink.search();
            if let Some(query_name) = &query_params.name {
                if !sink.conf.base.name.contains(query_name) {
                    continue;
                }
            }

            if total >= (pagination.page - 1) * pagination.size
                && total < pagination.page * pagination.size
            {
                data.push(sink);
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
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => sink.update(req).await,
            None => sink_not_found_err!(),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete().await?;
                self.sinks.retain(|sink| sink.id == sink_id);
                Ok(())
            }
            None => sink_not_found_err!(),
        }
    }

    async fn add_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.ref_info.add_ref(rule_id)),
            None => source_not_found_err!(),
        }
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.get_mb_rx(rule_id)),
            None => source_not_found_err!(),
        }
    }

    async fn del_source_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => {
                source.del_mb_rx(rule_id);
                Ok(())
            }
            None => source_not_found_err!(),
        }
    }

    async fn del_source_ref(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.ref_info.deactive_ref(rule_id)),
            None => source_not_found_err!(),
        }
    }

    async fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.add_ref(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    async fn get_sink_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => {
                if sink.stop_signal_tx.is_none() {
                    match self.ext_conf.version {
                        types::apps::mqtt_client::Version::V311 => {
                            sink.start_v311(self.client_v311.as_ref().unwrap().clone())
                        }
                        types::apps::mqtt_client::Version::V50 => {
                            sink.start_v50(self.client_v50.as_ref().unwrap().clone())
                        }
                    }
                }

                Ok(sink.get_mb_tx(rule_id))
            }
            None => sink_not_found_err!(),
        }
    }

    async fn del_sink_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.del_mb_tx(rule_id)),
            None => sink_not_found_err!(),
        }
    }

    async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.del_ref(rule_id)),
            None => sink_not_found_err!(),
        }
    }
}
