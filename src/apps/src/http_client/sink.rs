use std::sync::Arc;

use common::error::HaliaResult;
use message::MessageBatch;
use reqwest::Client;
use tokio::{select, sync::mpsc, task::JoinHandle};
use tracing::{trace, warn};
use types::apps::http_client::{HttpClientConf, SinkConf};

pub struct Sink {
    stop_signal_tx: mpsc::Sender<()>,
    join_handle: Option<
        JoinHandle<(
            mpsc::Receiver<()>,
            Arc<HttpClientConf>,
            SinkConf,
            Client,
            mpsc::Receiver<MessageBatch>,
        )>,
    >,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Sink {
    pub fn new(http_client_conf: Arc<HttpClientConf>, conf: SinkConf) -> Sink {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        let (mb_tx, mb_rx) = mpsc::channel(16);
        let join_handle =
            Self::event_loop(http_client_conf, conf, stop_signal_rx, mb_rx, Client::new());

        Sink {
            stop_signal_tx,
            join_handle: Some(join_handle),
            mb_tx,
        }
    }

    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn update_conf(&mut self, old_conf: SinkConf, new_conf: SinkConf) -> HaliaResult<()> {
        if old_conf == new_conf {
            return Ok(());
        }

        let (stop_signal_rx, http_client_conf, _, client, mb_rx) = self.stop().await;
        let join_handle =
            Self::event_loop(http_client_conf, new_conf, stop_signal_rx, mb_rx, client);
        self.join_handle = Some(join_handle);

        Ok(())
    }

    fn event_loop(
        http_client_conf: Arc<HttpClientConf>,
        conf: SinkConf,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
        client: Client,
    ) -> JoinHandle<(
        mpsc::Receiver<()>,
        Arc<HttpClientConf>,
        SinkConf,
        Client,
        mpsc::Receiver<MessageBatch>,
    )> {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, http_client_conf, conf, client, mb_rx);
                    }

                    mb = mb_rx.recv() => {
                        match mb {
                            Some(mb) => Sink::send_request(&client, &http_client_conf, &conf, mb).await,
                            None => warn!("http客户端收到空消息"),
                        }
                    }
                }
            }
        })
    }

    async fn send_request(
        client: &Client,
        http_client_conf: &Arc<HttpClientConf>,
        conf: &SinkConf,
        _mb: MessageBatch,
    ) {
        // todo 重复使用
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
        let (stop_signal_rx, _, conf, client, mb_rx) = self.stop().await;
        let join_handle = Self::event_loop(http_client_conf, conf, stop_signal_rx, mb_rx, client);
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(
        &mut self,
    ) -> (
        mpsc::Receiver<()>,
        Arc<HttpClientConf>,
        SinkConf,
        Client,
        mpsc::Receiver<MessageBatch>,
    ) {
        self.stop_signal_tx.send(()).await.unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }
}
