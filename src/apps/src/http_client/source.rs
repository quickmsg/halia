use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_info_resp,
    ref_info::RefInfo,
};
use message::MessageBatch;
use tokio::{
    select,
    sync::{broadcast, mpsc},
};
use tracing::{trace, warn};
use types::{
    apps::http_client::{HttpClientConf, SinkConf, SourceConf},
    BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,
    base_conf: BaseConf,
    ext_conf: SourceConf,
    on: bool,

    mb_tx: Option<broadcast::Sender<MessageBatch>>,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    pub ref_info: RefInfo,
}

impl Source {
    pub async fn new(source_id: Uuid, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<Source> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;

        Ok(Source {
            id: source_id,
            base_conf: req.base,
            ext_conf,
            ref_info: RefInfo::new(),
            on: false,
            stop_signal_tx: None,
            mb_tx: None,
        })
    }

    // pub fn check_duplicate(&self, req: &CreateUpdateSourceReq) -> HaliaResult<()> {
    //     if self.conf.base.name == req.base.name {
    //         return Err(HaliaError::NameExists);
    //     }

    //     Ok(())
    // }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        get_search_sources_or_sinks_info_resp!(self)
    }

    pub async fn update(&mut self, req: CreateUpdateSourceOrSinkReq) -> HaliaResult<()> {
        let ext_conf: SourceConf = serde_json::from_value(req.ext)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = req.base;
        self.ext_conf = ext_conf;

        if self.on && restart {}

        Ok(())
    }

    pub async fn delete(&mut self) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(HaliaError::DeleteRefing);
        }

        Ok(())
    }

    pub async fn start(&mut self, base_conf: HttpClientConf) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mb_rx) = broadcast::channel(16);
        self.mb_tx = Some(mb_tx);
        let conf = self.ext_conf.clone();
        self.event_loop(base_conf, stop_signal_rx, mb_rx, conf)
            .await;
    }

    // TODO
    async fn event_loop(
        &mut self,
        base_conf: HttpClientConf,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: broadcast::Receiver<MessageBatch>,
        conf: SourceConf,
    ) {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return
                    }

                    // mb = mb_rx.recv() => {
                    //     match mb {
                    //         Some(mb) => Sink::send_request(&base_conf.host, &conf, mb).await,
                    //         None => warn!("http客户端收到空消息"),
                    //     }
                    // }
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

    pub fn get_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        self.mb_tx.as_ref().unwrap().subscribe()
    }

    pub fn del_rx(&mut self, rule_id: &Uuid) {
        self.ref_info.deactive_ref(rule_id)
    }
}
