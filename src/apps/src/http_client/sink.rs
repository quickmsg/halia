use common::{error::HaliaResult, get_id, persistence, ref_info::RefInfo};
use message::MessageBatch;
use tokio::{select, sync::mpsc};
use tracing::{trace, warn};
use types::apps::http_client::{CreateUpdateSinkReq, SearchSinksItemResp, SinkConf};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    conf: CreateUpdateSinkReq,

    mb_tx: Option<mpsc::Sender<MessageBatch>>,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    ref_info: RefInfo,
}

pub async fn new(
    app_id: &Uuid,
    sink_id: Option<Uuid>,
    req: CreateUpdateSinkReq,
) -> HaliaResult<Sink> {
    let (sink_id, new) = get_id(sink_id);
    if new {
        persistence::apps::http_client::create_sink(
            app_id,
            &sink_id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;
    }

    Ok(Sink {
        id: sink_id,
        conf: req,
        ref_info: RefInfo::new(),
        stop_signal_tx: None,
        mb_tx: None,
    })
}

impl Sink {
    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, app_id: &Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        persistence::apps::http_client::update_sink(
            app_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;

        todo!()
    }

    pub async fn delete(&mut self, app_id: &Uuid) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(common::error::HaliaError::Common(
                "引用中，不能删除".to_owned(),
            ));
        }
        persistence::apps::http_client::delete_sink(app_id, &self.id).await?;

        Ok(())
    }

    pub async fn start(&mut self, host: String) {
        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mut mb_rx) = mpsc::channel(16);
        self.mb_tx = Some(mb_tx);
        let conf = self.conf.ext.clone();
    }

    async fn event_loop(
        host: String,
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
                            Some(mb) => Sink::send_request(&host, &conf, mb).await,
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

        match builder.send().await {
            Ok(_) => trace!("http client send ok"),
            Err(e) => warn!("http client send err:{:?}", e),
        }
    }

    pub async fn restart(&mut self) {}

    pub async fn stop(&mut self) {}

    pub fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.add_ref(rule_id);
    }

    pub fn get_mb_tx(&mut self, rule_id: &Uuid) -> mpsc::Sender<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        self.mb_tx.as_ref().unwrap().clone()
    }

    pub fn del_mb_tx(&mut self, rule_id: &Uuid) {
        self.ref_info.deactive_ref(rule_id);
    }

    pub fn del_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.del_ref(rule_id);
    }

    pub fn can_stop(&self) -> bool {
        self.ref_info.can_stop()
    }

    pub fn can_delete(&self) -> bool {
        self.ref_info.can_delete()
    }
}
