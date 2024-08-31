use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_info_resp,
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

    pub mb_tx: Option<broadcast::Sender<MessageBatch>>,

    stop_signal_tx: Option<mpsc::Sender<()>>,
}

impl Source {
    pub fn new(id: Uuid, base_conf: BaseConf, ext_conf: SourceConf) -> HaliaResult<Self> {
        Self::validate_conf(&ext_conf)?;
        Ok(Self {
            id,
            base_conf,
            ext_conf,
            stop_signal_tx: None,
            mb_tx: None,
        })
    }

    fn validate_conf(_conf: &SourceConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, _ext_conf: &SourceConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        get_search_sources_or_sinks_info_resp!(self)
    }

    pub async fn update(&mut self, base_conf: BaseConf, ext_conf: SourceConf) -> HaliaResult<()> {
        Self::validate_conf(&ext_conf)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        match (&self.stop_signal_tx, restart) {
            (None, true) => todo!(),
            (None, false) => todo!(),
            (Some(stop_singal_tx), true) => {
                todo!()
            }
            (Some(_), false) => todo!(),
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
}
