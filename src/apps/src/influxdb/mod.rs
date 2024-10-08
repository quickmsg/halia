use std::sync::Arc;

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use influxdb::Client as InfluxdbClientV1;
use influxdb2::Client as InfluxdbClientV2;
use message::MessageBatch;
use sink::Sink;
use tokio::sync::mpsc;
use types::apps::influxdb::{InfluxdbConf, SinkConf};

use crate::App;

mod sink;

pub struct Influxdb {
    _id: String,
    _err: Option<String>,
    sinks: DashMap<String, Sink>,
    conf: Arc<InfluxdbConf>,
}

pub enum InfluxdbClient {
    V1(InfluxdbClientV1),
    V2(InfluxdbClientV2),
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: InfluxdbConf = serde_json::from_value(conf).unwrap();

    Box::new(Influxdb {
        _id: id,
        _err: None,
        sinks: DashMap::new(),
        conf: Arc::new(conf),
    })
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: InfluxdbConf = serde_json::from_value(conf.clone())?;
    match conf.version {
        types::apps::influxdb::InfluxdbVersion::V1 => {
            if conf.v1.is_none() {
                return Err(HaliaError::Common("v1的配置为空！".to_owned()));
            }
        }
        types::apps::influxdb::InfluxdbVersion::V2 => {
            if conf.v2.is_none() {
                return Err(HaliaError::Common("v2的配置为空！".to_owned()));
            }
        }
    }
    Ok(())
}

pub fn validate_sink_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: SinkConf = serde_json::from_value(conf.clone())?;
    Sink::validate_conf(&conf)?;
    Ok(())
}

#[async_trait]
impl App for Influxdb {
    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let new_conf: InfluxdbConf = serde_json::from_value(new_conf)?;
        self.conf = Arc::new(new_conf);
        for mut sink in self.sinks.iter_mut() {
            sink.update_influxdb_client(self.conf.clone()).await;
        }

        Ok(())
    }

    async fn stop(&mut self) {
        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        match self.conf.version {
            types::apps::influxdb::InfluxdbVersion::V1 => {}
            types::apps::influxdb::InfluxdbVersion::V2 => {
                if conf["bucket"].is_null() {
                    return Err(HaliaError::Common("bucket为空！".to_owned()));
                }
            }
        }
        let conf: SinkConf = serde_json::from_value(conf)?;
        let sink = Sink::new(conf, self.conf.clone());
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: String,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let new_conf: SinkConf = serde_json::from_value(new_conf)?;
        match self.sinks.get_mut(&sink_id) {
            Some(mut sink) => {
                sink.update_conf(new_conf).await;
                Ok(())
            }
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

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.mb_tx.clone()),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

fn new_influxdb_client(influxdb_conf: &Arc<InfluxdbConf>, sink_conf: &SinkConf) -> InfluxdbClient {
    match &influxdb_conf.version {
        types::apps::influxdb::InfluxdbVersion::V1 => {
            let schema = match &influxdb_conf.conf_v1.as_ref().unwrap().ssl.enable {
                true => "https",
                false => "http",
            };
            let mut client = InfluxdbClientV1::new(
                format!(
                    "{}://{}:{}",
                    schema,
                    &influxdb_conf.conf_v1.as_ref().unwrap().host,
                    influxdb_conf.conf_v1.as_ref().unwrap().port
                ),
                &sink_conf.conf_v1.as_ref().unwrap().database,
            );

            match &sink_conf.conf_v1.as_ref().unwrap().auth.method {
                types::apps::influxdb::InfluxdbV1AuthMethod::None => {
                    match &influxdb_conf.v1.as_ref().unwrap().auth.method {
                        types::apps::influxdb::InfluxdbV1AuthMethod::None => {}
                        types::apps::influxdb::InfluxdbV1AuthMethod::BasicAuthentication => {
                            client = client.with_auth(
                                influxdb_conf
                                    .v1
                                    .as_ref()
                                    .unwrap()
                                    .auth
                                    .username
                                    .as_ref()
                                    .unwrap(),
                                influxdb_conf
                                    .v1
                                    .as_ref()
                                    .unwrap()
                                    .auth
                                    .password
                                    .as_ref()
                                    .unwrap(),
                            );
                        }
                        types::apps::influxdb::InfluxdbV1AuthMethod::ApiToken => {
                            client = client.with_token(
                                influxdb_conf
                                    .v1
                                    .as_ref()
                                    .unwrap()
                                    .auth
                                    .api_token
                                    .as_ref()
                                    .unwrap(),
                            );
                        }
                    }
                }
                types::apps::influxdb::InfluxdbV1AuthMethod::BasicAuthentication => {
                    client = client.with_auth(
                        sink_conf
                            .conf_v1
                            .as_ref()
                            .unwrap()
                            .auth
                            .username
                            .as_ref()
                            .unwrap(),
                        sink_conf
                            .conf_v1
                            .as_ref()
                            .unwrap()
                            .auth
                            .password
                            .as_ref()
                            .unwrap(),
                    );
                }
                types::apps::influxdb::InfluxdbV1AuthMethod::ApiToken => {
                    client = client.with_token(
                        sink_conf
                            .conf_v1
                            .as_ref()
                            .unwrap()
                            .auth
                            .api_token
                            .as_ref()
                            .unwrap(),
                    );
                }
            }

            InfluxdbClient::V1(client)
        }
        types::apps::influxdb::InfluxdbVersion::V2 => {
            let conf = influxdb_conf.v2.as_ref().unwrap();
            let client = InfluxdbClientV2::new(
                format!("{}:{}", &conf.url, conf.port),
                &conf.org,
                &conf.api_token,
            );
            InfluxdbClient::V2(client)
        }
    }
}
