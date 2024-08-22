use common::{error::HaliaResult, get_id, persistence, ref_info::RefInfo};
use message::MessageBatch;
use tokio::{select, sync::mpsc};
use tracing::{trace, warn};
use types::{
    apps::http_client::{HttpClientConf, SinkConf, SourceConf},
    BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksItemResp,
};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    base_conf: BaseConf,
    ext_conf: SinkConf,
    on: bool,

    mb_tx: Option<mpsc::Sender<MessageBatch>>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    pub ref_info: RefInfo,
}

impl Sink {
    pub async fn new(
        app_id: &Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<Sink> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;
        let (sink_id, new) = get_id(sink_id);
        if new {
            persistence::create_sink(app_id, &sink_id, &data).await?;
        }

        Ok(Sink {
            id: sink_id,
            base_conf,
            ext_conf,
            ref_info: RefInfo::new(),
            on: false,
            stop_signal_tx: None,
            mb_tx: None,
        })
    }

    fn parse_conf(req: CreateUpdateSourceOrSinkReq) -> HaliaResult<(BaseConf, SinkConf, String)> {
        let data = serde_json::to_string(&req)?;
        let conf: SinkConf = serde_json::from_value(req.ext)?;

        Ok((req.base, conf, data))
    }

    pub fn search(&self) -> SearchSourcesOrSinksItemResp {
        SearchSourcesOrSinksItemResp {
            id: self.id.clone(),
            conf: CreateUpdateSourceOrSinkReq {
                base: self.base_conf.clone(),
                ext: serde_json::to_value(self.ext_conf.clone()).unwrap(),
            },
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn update(
        &mut self,
        app_id: &Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<()> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;
        persistence::update_sink(app_id, &self.id, &data).await?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        if self.on && restart {}

        todo!()
    }

    pub async fn delete(&mut self, app_id: &Uuid) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(common::error::HaliaError::Common(
                "引用中，不能删除".to_owned(),
            ));
        }
        persistence::delete_sink(app_id, &self.id).await?;

        Ok(())
    }

    pub async fn start(&mut self, base_conf: HttpClientConf) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mb_rx) = mpsc::channel(16);
        self.mb_tx = Some(mb_tx);
        let conf = self.ext_conf.clone();
        self.event_loop(base_conf, stop_signal_rx, mb_rx, conf)
            .await;
    }

    async fn event_loop(
        &mut self,
        base_conf: HttpClientConf,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
        conf: SinkConf,
    ) {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return
                    }

                    mb = mb_rx.recv() => {
                        match mb {
                            Some(mb) => Sink::send_request(&base_conf.host, &conf, mb).await,
                            None => warn!("http客户端收到空消息"),
                        }
                    }
                }
            }
        });
    }

    async fn send_request(host: &String, conf: &SinkConf, mb: MessageBatch) {
        let client = reqwest::Client::new();
        let mut builder = match conf.method {
            types::apps::http_client::SinkMethod::Get => client.post(host),
            types::apps::http_client::SinkMethod::Post => client.post(host),
            types::apps::http_client::SinkMethod::Delete => client.delete(host),
            types::apps::http_client::SinkMethod::Patch => client.patch(host),
            types::apps::http_client::SinkMethod::Put => client.put(host),
            types::apps::http_client::SinkMethod::Head => client.head(host),
        };

        builder = builder.query(&conf.query_params);

        for (k, v) in conf.headers.iter() {
            builder = builder.header(k, v);
        }

        // builder.body();

        match builder.send().await {
            Ok(_) => trace!("http client send ok"),
            Err(e) => warn!("http client send err:{:?}", e),
        }
    }

    pub async fn restart(&mut self) {}

    pub async fn stop(&mut self) {}

    pub fn get_tx(&mut self, rule_id: &Uuid) -> mpsc::Sender<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        self.mb_tx.as_ref().unwrap().clone()
    }

    pub fn del_tx(&mut self, rule_id: &Uuid) {
        self.ref_info.deactive_ref(rule_id);
    }
}
