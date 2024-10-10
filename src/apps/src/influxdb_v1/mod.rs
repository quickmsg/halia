use std::sync::Arc;

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use sink::Sink;
use tokio::sync::mpsc;
use types::apps::influxdb_v1::{Conf, SinkConf};

use crate::App;

mod sink;

pub struct Influxdb {
    _id: String,
    _err: Option<String>,
    sinks: DashMap<String, Sink>,
    conf: Arc<Conf>,
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: Conf = serde_json::from_value(conf).unwrap();

    Box::new(Influxdb {
        _id: id,
        _err: None,
        sinks: DashMap::new(),
        conf: Arc::new(conf),
    })
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: Conf = serde_json::from_value(conf.clone())?;
    match conf.auth_method {
        types::apps::influxdb_v1::AuthMethod::None => {}
        types::apps::influxdb_v1::AuthMethod::Password => {
            if conf.auth_password.is_none() {
                return Err(HaliaError::Common("auth_password字段不能为空！".to_owned()));
            }
        }
        types::apps::influxdb_v1::AuthMethod::ApiToken => {
            if conf.auth_api_token.is_none() {
                return Err(HaliaError::Common(
                    "auth_api_token字段不能为空！".to_owned(),
                ));
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
        let new_conf: Conf = serde_json::from_value(new_conf)?;
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