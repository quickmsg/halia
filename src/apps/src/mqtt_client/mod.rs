use std::{str::FromStr, sync::Arc, time::Duration};

use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    get_id, persistence,
};
use message::MessageBatch;
use rumqttc::{v5, AsyncClient, Event, Incoming, MqttOptions, QoS};
use sink::Sink;
use source::Source;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    time,
};
use tracing::error;
use types::{
    apps::{
        mqtt_client::{
            CreateUpdateMqttClientReq, CreateUpdateSinkReq, CreateUpdateSourceReq, Qos,
            SearchSinksResp, SearchSourcesResp,
        },
        SearchAppsItemConf, SearchAppsItemResp,
    },
    Pagination,
};
use uuid::Uuid;

pub const TYPE: &str = "mqtt_client";

pub mod manager;
mod sink;
mod source;

pub struct MqttClient {
    pub id: Uuid,
    conf: CreateUpdateMqttClientReq,

    on: bool,
    err: Arc<RwLock<Option<String>>>,
    stop_signal_tx: Option<mpsc::Sender<()>>,

    sources: Arc<RwLock<Vec<Source>>>,
    sinks: Vec<Sink>,
    client_v311: Option<Arc<AsyncClient>>,
    client_v50: Option<Arc<v5::AsyncClient>>,
}

macro_rules! source_not_found_err {
    ($source_id:expr) => {
        Err(HaliaError::NotFound("mqtt客户端源".to_owned(), $source_id))
    };
}

macro_rules! sink_not_found_err {
    ($sink_id:expr) => {
        Err(HaliaError::NotFound("mqtt客户端动作".to_owned(), $sink_id))
    };
}

impl MqttClient {
    pub async fn new(app_id: Option<Uuid>, req: CreateUpdateMqttClientReq) -> HaliaResult<Self> {
        let (app_id, new) = get_id(app_id);
        if new {
            persistence::apps::mqtt_client::create(
                &app_id,
                TYPE,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: app_id,
            conf: req,
            on: false,
            err: Arc::new(RwLock::new(None)),
            sources: Arc::new(RwLock::new(vec![])),
            sinks: vec![],
            client_v311: None,
            client_v50: None,
            stop_signal_tx: None,
        })
    }

    pub async fn recover(&mut self) -> HaliaResult<()> {
        match persistence::apps::mqtt_client::read_sources(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);
                    let source_id = Uuid::from_str(items[0]).unwrap();
                    self.create_source(Some(source_id), serde_json::from_str(items[1]).unwrap())
                        .await?;
                }
            }
            Err(e) => return Err(e.into()),
        }

        match persistence::apps::mqtt_client::read_sinks(&self.id).await {
            Ok(datas) => {
                for data in datas {
                    if data.len() == 0 {
                        continue;
                    }
                    let items = data.split(persistence::DELIMITER).collect::<Vec<&str>>();
                    assert_eq!(items.len(), 2);
                    let sink_id = Uuid::from_str(items[0]).unwrap();
                    self.create_sink(Some(sink_id), serde_json::from_str(items[1]).unwrap())
                        .await?;
                }
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    pub async fn update(&mut self, req: CreateUpdateMqttClientReq) -> HaliaResult<()> {
        persistence::apps::update_app_conf(&self.id, serde_json::to_string(&req).unwrap()).await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;

        if self.on && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            self.start().await.unwrap();
            for sink in self.sinks.iter_mut() {
                match self.conf.ext.version {
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

    pub async fn start(&mut self) -> HaliaResult<()> {
        check_and_set_on_true!(self);

        persistence::apps::update_app_status(&self.id, persistence::Status::Runing).await?;

        match self.conf.ext.version {
            types::apps::mqtt_client::Version::V311 => self.start_v311().await,
            types::apps::mqtt_client::Version::V50 => self.start_v50().await,
        }

        Ok(())
    }

    async fn start_v311(&mut self) {
        let mut mqtt_options = MqttOptions::new(
            self.conf.ext.client_id.clone(),
            self.conf.ext.host.clone(),
            self.conf.ext.port,
        );

        mqtt_options.set_keep_alive(Duration::from_secs(self.conf.ext.keep_alive));

        match (&self.conf.ext.username, &self.conf.ext.password) {
            (Some(username), Some(password)) => {
                mqtt_options.set_credentials(username.clone(), password.clone());
            }
            (_, _) => {}
        }

        let (client, mut event_loop) = AsyncClient::new(mqtt_options, 16);

        let err = self.err.clone();

        // match (
        //     &self.conf.ext.ca,
        //     &self.conf.ext.client_cert,
        //     &self.conf.ext.client_key,
        // ) {
        //     (Some(ca), Some(client_cert), Some(client_key)) => {
        //         let mut root_store = RootCertStore::empty();
        //         // root_store.add(ca);
        //     }
        //     _ => {}
        // }

        let sources = self.sources.clone();
        for source in sources.read().await.iter() {
            let _ = client
                .subscribe(
                    source.conf.ext.topic.clone(),
                    get_mqtt_qos(&source.conf.ext.qos),
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
                        match event {
                            Ok(Event::Incoming(Incoming::Publish(p))) => {
                                match MessageBatch::from_json(p.payload) {
                                    Ok(msg) => {
                                        for source in sources.write().await.iter_mut() {
                                            if matches(&source.conf.ext.topic, &p.topic) {
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
                                }
                            }
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
                }
            }
        });
    }

    async fn start_v50(&mut self) {
        let mut mqtt_options = v5::MqttOptions::new(
            self.conf.ext.client_id.clone(),
            self.conf.ext.host.clone(),
            self.conf.ext.port,
        );
        mqtt_options.set_keep_alive(Duration::from_secs(self.conf.ext.keep_alive));

        if self.conf.ext.username.is_some() && self.conf.ext.password.is_some() {
            mqtt_options.set_credentials(
                self.conf.ext.username.as_ref().unwrap().clone(),
                self.conf.ext.password.as_ref().unwrap().clone(),
            );
        }

        let (client, mut event_loop) = v5::AsyncClient::new(mqtt_options, 16);
        let sources = self.sources.clone();
        for source in sources.read().await.iter() {
            // let _ = client
            //     .subscribe(
            //         source.conf.ext.topic.clone(),
            //         v5::mqttbytes::qos(source.conf.ext.qos).unwrap(),
            //     )
            //     .await;
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
                        match event {
                            Ok(v5::Event::Incoming(v5::Incoming::Publish(p))) => {
                                match MessageBatch::from_json(p.payload) {
                                    Ok(msg) => {
                                        // for source in sources.write().await.iter_mut() {
                                        //     if matches(&source.conf.topic, p.topic) {
                                        //         match &source.tx {
                                        //             Some(tx) => {
                                        //                 let _ = tx.send(msg.clone());
                                        //             }
                                        //             None => {}
                                        //         }
                                        //     }
                                        // }
                                    }
                                    Err(e) => error!("Failed to decode msg:{}", e),
                                }
                            }
                            Ok(_) => (),
                            Err(e) => {
                                match e {
                                    v5::ConnectionError::MqttState(_) => todo!(),
                                    v5::ConnectionError::Timeout(_) => todo!(),
                                    v5::ConnectionError::Tls(_) => todo!(),
                                    v5::ConnectionError::Io(_) => todo!(),
                                    v5::ConnectionError::ConnectionRefused(_) => todo!(),
                                    v5::ConnectionError::NotConnAck(_) => todo!(),
                                    v5::ConnectionError::RequestsDone => todo!(),
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
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

        persistence::apps::update_app_status(&self.id, persistence::Status::Stopped).await?;

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
        match self.conf.ext.version {
            types::apps::mqtt_client::Version::V311 => self.client_v311 = None,
            types::apps::mqtt_client::Version::V50 => self.client_v50 = None,
        }

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
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

        if self.sinks.iter().any(|sink| sink.ref_info.can_delete()) {
            return Err(HaliaError::Common("有规则正在被引用中".to_owned()));
        }

        persistence::apps::delete_app(&self.id).await?;
        Ok(())
    }

    async fn search(&self) -> SearchAppsItemResp {
        SearchAppsItemResp {
            id: self.id,
            on: self.on,
            typ: TYPE,
            conf: SearchAppsItemConf {
                base: self.conf.base.clone(),
                ext: serde_json::json!(self.conf.ext),
            },
            err: self.err.read().await.clone(),
        }
    }

    async fn get_source_mb_rx(
        &mut self,
        source_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        let rx = match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => source.get_mb_rx(rule_id),
            None => return Err(HaliaError::NotFound("源".to_owned(), source_id.clone())),
        };

        Ok(rx)
    }

    async fn del_source_mb_rx(&mut self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
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
            None => Err(HaliaError::NotFound("源".to_owned(), source_id.clone())),
        }
    }

    pub async fn create_source(
        &self,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceReq,
    ) -> HaliaResult<()> {
        match Source::new(&self.id, source_id, req).await {
            Ok(source) => {
                if self.stop_signal_tx.is_some() {
                    match self.conf.ext.version {
                        types::apps::mqtt_client::Version::V311 => {
                            if let Err(e) = self
                                .client_v311
                                .as_ref()
                                .unwrap()
                                .subscribe(
                                    source.conf.ext.topic.clone(),
                                    get_mqtt_qos(&source.conf.ext.qos),
                                )
                                .await
                            {
                                error!("client subscribe err:{e}");
                            }
                        }
                        types::apps::mqtt_client::Version::V50 => {
                            // if let Err(e) = self
                            //     .client_v50
                            //     .as_ref()
                            //     .unwrap()
                            //     .subscribe(
                            //         source.conf.ext.topic.clone(),
                            //         v5::mqttbytes::qos(source.conf.ext.qos).unwrap(),
                            //     )
                            //     .await
                            // {
                            //     error!("client subscribe err:{e}");
                            // }
                            // todo
                        }
                    }
                }
                self.sources.write().await.push(source);

                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sources(&self, pagination: Pagination) -> HaliaResult<SearchSourcesResp> {
        let mut data = vec![];
        for source in self
            .sources
            .read()
            .await
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(source.search());
            if data.len() == pagination.size {
                break;
            }
        }

        Ok(SearchSourcesResp {
            total: self.sources.read().await.len(),
            data,
        })
    }

    async fn update_source(&self, source_id: Uuid, req: CreateUpdateSourceReq) -> HaliaResult<()> {
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => match source.update(&self.id, req).await {
                Ok(restart) => {
                    if self.stop_signal_tx.is_some() && restart {
                        match self.conf.ext.version {
                            types::apps::mqtt_client::Version::V311 => {
                                if let Err(e) = self
                                    .client_v311
                                    .as_ref()
                                    .unwrap()
                                    .unsubscribe(source.conf.ext.topic.clone())
                                    .await
                                {
                                    error!("unsubscribe err:{e}");
                                }

                                if let Err(e) = self
                                    .client_v311
                                    .as_ref()
                                    .unwrap()
                                    .subscribe(
                                        source.conf.ext.topic.clone(),
                                        get_mqtt_qos(&source.conf.ext.qos),
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
                                    .unsubscribe(source.conf.ext.topic.clone())
                                    .await
                                {
                                    error!("unsubscribe err:{e}");
                                }

                                // if let Err(e) = self
                                //     .client_v50
                                //     .as_ref()
                                //     .unwrap()
                                //     .subscribe(
                                //         source.conf.ext.topic.clone(),
                                //         v5::mqttbytes::qos(source.conf.ext.qos).unwrap(),
                                //     )
                                //     .await
                                // {
                                //     error!("subscribe err:{e}");
                                // }
                            }
                        }
                    }

                    Ok(())
                }
                Err(e) => Err(e),
            },
            None => source_not_found_err!(source_id),
        }
    }

    async fn delete_source(&self, source_id: Uuid) -> HaliaResult<()> {
        match self
            .sources
            .read()
            .await
            .iter()
            .find(|source| source.id == source_id)
        {
            Some(source) => source.delete(&self.id).await,
            None => source_not_found_err!(source_id),
        }
    }

    pub async fn add_source_ref(&self, source_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self
            .sources
            .write()
            .await
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.add_ref(rule_id)),
            None => source_not_found_err!(source_id.clone()),
        }
    }

    async fn create_sink(
        &mut self,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match Sink::new(&self.id, sink_id, req).await {
            Ok(sink) => {
                self.sinks.push(sink);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    async fn search_sinks(&self, pagination: Pagination) -> SearchSinksResp {
        let mut data = vec![];
        for sink in self
            .sinks
            .iter()
            .rev()
            .skip((pagination.page - 1) * pagination.size)
        {
            data.push(sink.search());
            if data.len() == pagination.size {
                break;
            }
        }
        SearchSinksResp {
            total: self.sinks.len(),
            data,
        }
    }

    pub async fn update_sink(
        &mut self,
        sink_id: Uuid,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => match sink.update(&self.id, req).await {
                Ok(restart) => {
                    if sink.stop_signal_tx.is_some() && restart {
                        match self.conf.ext.version {
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
                    Ok(())
                }
                Err(e) => Err(e),
            },
            None => sink_not_found_err!(sink_id),
        }
    }

    pub async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
            Some(sink) => {
                sink.delete(&self.id).await?;
                self.sinks.retain(|sink| sink.id == sink_id);
                Ok(())
            }
            None => sink_not_found_err!(sink_id),
        }
    }

    pub fn add_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.add_ref(rule_id)),
            None => sink_not_found_err!(sink_id.clone()),
        }
    }

    pub async fn get_sink_mb_tx(
        &mut self,
        sink_id: &Uuid,
        rule_id: &Uuid,
    ) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => {
                if sink.stop_signal_tx.is_none() {
                    match self.conf.ext.version {
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
            None => sink_not_found_err!(sink_id.clone()),
        }
    }

    pub async fn del_sink_mb_tx(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.del_mb_tx(rule_id)),
            None => sink_not_found_err!(sink_id.clone()),
        }
    }

    pub async fn del_sink_ref(&mut self, sink_id: &Uuid, rule_id: &Uuid) -> HaliaResult<()> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.ref_info.del_ref(rule_id)),
            None => sink_not_found_err!(sink_id.clone()),
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

fn get_mqtt_qos(qos: &Qos) -> QoS {
    match qos {
        Qos::AtMostOnce => QoS::AtMostOnce,
        Qos::AtLeastOnce => QoS::AtLeastOnce,
        Qos::ExactlyOnce => QoS::ExactlyOnce,
    }
}
