use std::sync::Arc;

use async_trait::async_trait;
use common::{
    error::{HaliaError, HaliaResult},
    ref_info::RefInfo,
};
use message::MessageBatch;
use sink::Sink;
use source::Source;
use tokio::sync::{broadcast, mpsc};
use types::{
    apps::{
        http_client::{HttpClientConf, SinkConf, SourceConf},
        AppConf, AppType, CreateUpdateAppReq,
    },
    BaseConf, CreateUpdateSourceOrSinkReq,
};
use uuid::Uuid;

use crate::App;

mod sink;
mod source;

pub struct HttpClient {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: Arc<HttpClientConf>,

    on: bool,
    // err: Option<String>,
    sources: Vec<Source>,
    source_ref_infos: Vec<(Uuid, RefInfo)>,
    sinks: Vec<Sink>,
    sink_ref_infos: Vec<(Uuid, RefInfo)>,
}

pub fn new(app_id: Uuid, app_conf: AppConf) -> HaliaResult<Box<dyn App>> {
    let ext_conf: HttpClientConf = serde_json::from_value(app_conf.ext)?;
    HttpClient::validate_conf(&ext_conf)?;

    Ok(Box::new(HttpClient {
        id: app_id,
        base_conf: app_conf.base,
        ext_conf: Arc::new(ext_conf),
        on: false,
        // err: None,
        sources: vec![],
        source_ref_infos: vec![],
        sinks: vec![],
        sink_ref_infos: vec![],
    }))
}

impl HttpClient {
    fn validate_conf(_conf: &HttpClientConf) -> HaliaResult<()> {
        Ok(())
    }
}

#[async_trait]
impl App for HttpClient {
    fn check_duplicate(&self, req: &CreateUpdateAppReq) -> HaliaResult<()> {
        if self.base_conf.name == req.conf.base.name {
            return Err(HaliaError::NameExists);
        }

        if req.app_type == AppType::HttpClient {
            let ext_conf: HttpClientConf = serde_json::from_value(req.conf.ext.clone())?;
            if self.ext_conf.host == ext_conf.host && self.ext_conf.port == ext_conf.port {
                return Err(HaliaError::AddressExists);
            }
        }

        Ok(())
    }

    async fn update(&mut self, app_conf: AppConf) -> HaliaResult<()> {
        let ext_conf: HttpClientConf = serde_json::from_value(app_conf.ext)?;
        HttpClient::validate_conf(&ext_conf)?;

        let mut restart = false;
        if *self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = app_conf.base;
        self.ext_conf = Arc::new(ext_conf);

        if self.on && restart {
            for source in self.sources.iter_mut() {
                source.update_http_client(self.ext_conf.clone()).await;
            }

            for sink in self.sinks.iter_mut() {
                sink.update_http_client(self.ext_conf.clone()).await;
            }
        }

        Ok(())
    }

    async fn stop(&mut self) -> HaliaResult<()> {
        for source in self.sources.iter_mut() {
            source.stop().await;
        }
        for sink in self.sinks.iter_mut() {
            sink.stop().await;
        }

        Ok(())
    }

    async fn create_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;

        for source in self.sources.iter() {
            source.check_duplicate(&req.base, &ext_conf)?;
        }

        let mut source = Source::new(source_id, req.base, ext_conf)?;
        if self.on {
            source.start(self.ext_conf.clone()).await;
        }

        self.sources.push(source);
        self.source_ref_infos.push((source_id, RefInfo::new()));

        Ok(())
    }

    async fn update_source(
        &mut self,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;

        for source in self.sources.iter() {
            if source.id != source_id {
                source.check_duplicate(&req.base, &ext_conf)?;
            }
        }

        match self
            .sources
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => source.update_conf(req.base, ext_conf).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn delete_source(&mut self, source_id: Uuid) -> HaliaResult<()> {
        match self
            .sources
            .iter_mut()
            .find(|source| source.id == source_id)
        {
            Some(source) => source.stop().await,
            None => unreachable!(),
        }

        self.sources.retain(|source| source.id != source_id);
        self.source_ref_infos.retain(|(id, _)| *id != source_id);

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

        let mut sink = Sink::new(sink_id, req.base, ext_conf)?;
        if self.on {
            sink.start(self.ext_conf.clone()).await;
        }

        self.sinks.push(sink);
        self.sink_ref_infos.push((sink_id, RefInfo::new()));

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
            Some(sink) => sink.update_conf(req.base, ext_conf).await,
            None => Err(HaliaError::NotFound),
        }
    }

    async fn delete_sink(&mut self, sink_id: Uuid) -> HaliaResult<()> {
        if self.on {
            match self.sinks.iter_mut().find(|sink| sink.id == sink_id) {
                Some(sink) => sink.stop().await,
                None => unreachable!(),
            }
        }

        self.sinks.retain(|sink| sink.id != sink_id);
        self.sink_ref_infos.retain(|(id, _)| *id != sink_id);
        Ok(())
    }

    async fn get_source_rx(
        &mut self,
        source_id: &Uuid,
    ) -> HaliaResult<broadcast::Receiver<MessageBatch>> {
        match self
            .sources
            .iter_mut()
            .find(|source| source.id == *source_id)
        {
            Some(source) => Ok(source.mb_tx.as_ref().unwrap().subscribe()),
            None => unreachable!(),
        }
    }

    async fn get_sink_tx(&mut self, sink_id: &Uuid) -> HaliaResult<mpsc::Sender<MessageBatch>> {
        match self.sinks.iter_mut().find(|sink| sink.id == *sink_id) {
            Some(sink) => Ok(sink.mb_tx.as_ref().unwrap().clone()),
            None => unreachable!(),
        }
    }
}
