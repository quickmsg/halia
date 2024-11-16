use std::sync::Arc;

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::RuleMessageBatch;
use sink::Sink;
use taos::{AsyncQueryable, AsyncTBuilder, Taos, TaosBuilder};
use tokio::sync::mpsc;
use tracing::warn;
use types::apps::tdengine::{SinkConf, TDengineConf};

use crate::App;

mod sink;

pub struct TDengine {
    _id: String,
    conf: Arc<TDengineConf>,
    sinks: DashMap<String, Sink>,
    err: Option<String>,
}

pub fn validate_conf(conf: &serde_json::Value) -> HaliaResult<()> {
    let conf: TDengineConf = serde_json::from_value(conf.clone())?;
    match conf.auth_method {
        types::apps::tdengine::TDengineAuthMethod::None => {}
        types::apps::tdengine::TDengineAuthMethod::Password => {
            if conf.auth_password.is_none() {
                return Err(HaliaError::Common("密码为空！".to_owned()));
            }
        }
    }
    Ok(())
}

pub fn validate_sink_conf(_conf: &serde_json::Value) -> HaliaResult<()> {
    Ok(())
}

pub fn new(id: String, conf: serde_json::Value) -> Box<dyn App> {
    let conf: TDengineConf = serde_json::from_value(conf).unwrap();

    Box::new(TDengine {
        _id: id,
        conf: Arc::new(conf),
        sinks: DashMap::new(),
        err: None,
    })
}

#[async_trait]
impl App for TDengine {
    async fn read_app_err(&self) -> Option<String> {
        self.err.clone()
    }

    async fn update(
        &mut self,
        _old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let new_conf: Arc<TDengineConf> = Arc::new(serde_json::from_value(new_conf.clone())?);
        for mut sink in self.sinks.iter_mut() {
            sink.update_tdengine_conf(new_conf.clone()).await;
        }
        self.conf = new_conf;
        Ok(())
    }

    async fn stop(&mut self) {
        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        let conf: SinkConf = serde_json::from_value(conf.clone())?;
        let sink = Sink::new(conf, self.conf.clone()).await;
        self.sinks.insert(sink_id, sink);
        Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        match self.sinks.get_mut(&sink_id) {
            Some(mut sink) => {
                let old_conf: SinkConf = serde_json::from_value(old_conf)?;
                let new_conf: SinkConf = serde_json::from_value(new_conf)?;
                sink.update_conf(old_conf, new_conf).await;
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

    async fn get_sink_txs(
        &self,
        sink_id: &String,
        cnt: usize,
    ) -> HaliaResult<Vec<mpsc::UnboundedSender<RuleMessageBatch>>> {
        match self.sinks.get(sink_id) {
            Some(sink) => Ok(sink.get_txs(cnt)),
            None => Err(HaliaError::NotFound(sink_id.to_owned())),
        }
    }
}

async fn new_tdengine_client(td_engine_conf: &Arc<TDengineConf>, sink_conf: &SinkConf) -> Taos {
    let dsn = match td_engine_conf.auth_method {
        types::apps::tdengine::TDengineAuthMethod::None => {
            format!("ws://{}:{}", td_engine_conf.host, td_engine_conf.port)
        }
        types::apps::tdengine::TDengineAuthMethod::Password => {
            let auth_password = td_engine_conf.auth_password.as_ref().unwrap();
            format!(
                "ws://{}:{}@{}:{}",
                auth_password.username,
                auth_password.password,
                td_engine_conf.host,
                td_engine_conf.port
            )
        }
    };
    let taos = TaosBuilder::from_dsn(dsn).unwrap().build().await.unwrap();
    if let Err(e) = taos.exec(format!("USE `{}`", sink_conf.db)).await {
        warn!("{}", e);
    }

    taos
}
