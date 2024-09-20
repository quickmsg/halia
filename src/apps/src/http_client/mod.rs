use std::sync::Arc;

use async_trait::async_trait;
use common::error::{HaliaError, HaliaResult};
use dashmap::DashMap;
use message::MessageBatch;
use sink::Sink;
use source::Source;
use tokio::sync::{broadcast, mpsc};
use types::apps::{
    http_client::{HttpClientConf, SourceConf},
    AppConf,
};

use crate::App;

mod sink;
mod source;

pub struct HttpClient {
    pub id: String,

    conf: Arc<HttpClientConf>,

    // err: Option<String>,
    sources: DashMap<String, Source>,
    sinks: DashMap<String, Sink>,
}

pub fn new(app_id: String, app_conf: AppConf) -> HaliaResult<Box<dyn App>> {
    let ext_conf: HttpClientConf = serde_json::from_value(app_conf.ext)?;
    HttpClient::validate_conf(&ext_conf)?;

    Ok(Box::new(HttpClient {
        id: app_id,
        conf: Arc::new(ext_conf),
        // err: None,
        sources: DashMap::new(),
        sinks: DashMap::new(),
    }))
}

impl HttpClient {
    fn validate_conf(_conf: &HttpClientConf) -> HaliaResult<()> {
        Ok(())
    }
}

#[async_trait]
impl App for HttpClient {
    async fn update(&mut self, app_conf: AppConf) -> HaliaResult<()> {
        let ext_conf: HttpClientConf = serde_json::from_value(app_conf.ext)?;
        HttpClient::validate_conf(&ext_conf)?;

        let mut restart = false;
        if *self.conf != ext_conf {
            restart = true;
        }
        self.conf = Arc::new(ext_conf);

        if restart {
            for mut source in self.sources.iter_mut() {
                source.update_http_client(self.conf.clone()).await;
            }

            for mut sink in self.sinks.iter_mut() {
                sink.update_http_client(self.conf.clone()).await;
            }
        }

        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        for mut source in self.sources.iter_mut() {
            source.stop().await;
        }
        for mut sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: String,
        conf: serde_json::Value,
    ) -> HaliaResult<()> {
        let conf: SourceConf = serde_json::from_value(conf)?;

        // for source in self.sources.iter() {
        //     source.check_duplicate(&req.base, &ext_conf)?;
        // }

        // let mut source = Source::new(source_id, req.base, ext_conf)?;
        // if self.on {
        //     source.start(self.ext_conf.clone()).await;
        // }

        // self.sources.push(source);
        // self.source_ref_infos.push((source_id, RefInfo::new()));

        Ok(())
    }

    async fn update_source(
        &mut self,
        source_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        // let ext_conf: SourceConf = serde_json::from_value(req.ext)?;

        // for source in self.sources.iter() {
        //     if source.id != source_id {
        //         source.check_duplicate(&req.base, &ext_conf)?;
        //     }
        // }

        // match self
        //     .sources
        //     .iter_mut()
        //     .find(|source| source.id == source_id)
        // {
        //     Some(source) => source.update_conf(req.base, ext_conf).await,
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    async fn delete_source(&mut self, source_id: String) -> HaliaResult<()> {
        self.sources
            .get_mut(&source_id)
            .ok_or(HaliaError::NotFound)?
            .stop()
            .await;

        self.sources.remove(&source_id);

        Ok(())
    }

    async fn create_sink(&mut self, sink_id: String, conf: serde_json::Value) -> HaliaResult<()> {
        // let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        // for sink in self.sinks.iter() {
        //     sink.check_duplicate(&req.base, &ext_conf)?;
        // }

        // let mut sink = Sink::new(sink_id, req.base, ext_conf)?;
        // if self.on {
        //     sink.start(self.ext_conf.clone()).await;
        // }

        // self.sinks.push(sink);
        // self.sink_ref_infos.push((sink_id, RefInfo::new()));

        Ok(())
    }

    async fn update_sink(
        &mut self,
        sink_id: String,
        old_conf: serde_json::Value,
        new_conf: serde_json::Value,
    ) -> HaliaResult<()> {
        // let ext_conf: SinkConf = serde_json::from_value(req.ext)?;
        // for sink in self.sinks.iter() {
        //     if sink.id != sink_id {
        //         sink.check_duplicate(&req.base, &ext_conf)?;
        //     }
        // }

        // match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
        //     Some(sink) => sink.update_conf(req.base, ext_conf).await,
        //     None => Err(HaliaError::NotFound),
        // }
        todo!()
    }

    async fn delete_sink(&mut self, sink_id: String) -> HaliaResult<()> {
        self.sinks
            .get_mut(&sink_id)
            .ok_or(HaliaError::NotFound)?
            .stop()
            .await;

        self.sinks.remove(&sink_id);

        Ok(())
    }

    async fn get_source_rx(
        &self,
        source_id: &String,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        Ok(self
            .sources
            .get(source_id)
            .ok_or(HaliaError::NotFound)?
            .mb_tx
            .subscribe())
    }

    async fn get_sink_tx(&self, sink_id: &String) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        Ok(self
            .sinks
            .get_mut(sink_id)
            .ok_or(HaliaError::NotFound)?
            .mb_tx
            .clone())
    }
}
