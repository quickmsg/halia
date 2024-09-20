use std::sync::Arc;

use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use reqwest::Client;
use tokio::{select, sync::mpsc, task::JoinHandle};
use tracing::{trace, warn};
use types::{
    apps::http_client::{HttpClientConf, SinkConf},
    BaseConf, SearchSourcesOrSinksInfoResp,
};

pub struct Sink {
    base_conf: BaseConf,
    ext_conf: Arc<SinkConf>,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            Arc<HttpClientConf>,
            Client,
            mpsc::Receiver<MessageBatch>,
        )>,
    >,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Sink {
    pub fn new(base_conf: BaseConf, ext_conf: SinkConf) -> HaliaResult<Sink> {
        Self::validate_conf(&ext_conf)?;

        let (mb_tx, mb_rx) = mpsc::channel(16);

        Ok(Sink {
            base_conf,
            ext_conf: Arc::new(ext_conf),
            stop_signal_tx: None,
            join_handle: None,
            mb_tx,
        })
    }

    fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, _ext_conf: &SinkConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        todo!()
    }

    pub async fn update_conf(
        &mut self,
        base_conf: BaseConf,
        ext_conf: SinkConf,
    ) -> HaliaResult<()> {
        self.base_conf = base_conf;
        if *self.ext_conf == ext_conf {
            return Ok(());
        }

        match &self.stop_signal_tx {
            Some(_) => todo!(),
            None => todo!(),
        }
    }

    pub async fn start(&mut self, http_client_conf: Arc<HttpClientConf>) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mb_rx) = mpsc::channel(16);
        self.mb_tx = mb_tx;
        self.event_loop(
            http_client_conf,
            stop_signal_rx,
            mb_rx,
            reqwest::Client::new(),
        )
        .await;
    }

    async fn event_loop(
        &mut self,
        http_client_conf: Arc<HttpClientConf>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
        client: Client,
    ) {
        let ext_conf = self.ext_conf.clone();
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, http_client_conf, client, mb_rx);
                    }

                    mb = mb_rx.recv() => {
                        match mb {
                            Some(mb) => Sink::send_request(&client, &http_client_conf, &ext_conf, mb).await,
                            None => warn!("http客户端收到空消息"),
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    async fn send_request(
        client: &Client,
        http_client_conf: &Arc<HttpClientConf>,
        conf: &Arc<SinkConf>,
        _mb: MessageBatch,
    ) {
        let url = format!("{}{}", &http_client_conf.host, &conf.path);

        let mut builder = match conf.method {
            types::apps::http_client::SinkMethod::Get => client.get(url),
            types::apps::http_client::SinkMethod::Post => client.post(url),
            types::apps::http_client::SinkMethod::Delete => client.delete(url),
            types::apps::http_client::SinkMethod::Patch => client.patch(url),
            types::apps::http_client::SinkMethod::Put => client.put(url),
            types::apps::http_client::SinkMethod::Head => client.head(url),
        };

        builder = builder.query(&conf.query_params);

        for (k, v) in conf.headers.iter() {
            builder = builder.header(k, v);
        }

        let request = builder.build().unwrap();

        match client.execute(request).await {
            Ok(_) => trace!("http client send ok"),
            Err(e) => warn!("http client send err:{:?}", e),
        }
    }

    pub async fn update_http_client(&mut self, http_client_conf: Arc<HttpClientConf>) {
        self.stop().await;
        let (stop_signal_rx, _, client, mb_rx) = self.join_handle.take().unwrap().await.unwrap();
        self.event_loop(http_client_conf, stop_signal_rx, mb_rx, client)
            .await;
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
    }
}
