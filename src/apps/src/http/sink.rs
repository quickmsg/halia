use std::sync::Arc;

use common::error::HaliaResult;
use message::RuleMessageBatch;
use reqwest::Client;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use tracing::{trace, warn};
use types::apps::http_client::{HttpClientConf, SinkConf};

use super::{build_headers, build_http_client};

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: UnboundedSender<RuleMessageBatch>,
}

pub struct JoinHandleData {
    pub stop_signal_rx: watch::Receiver<()>,
    pub http_client_conf: Arc<HttpClientConf>,
    pub conf: SinkConf,
    pub client: Client,
    pub mb_rx: UnboundedReceiver<RuleMessageBatch>,
}

impl Sink {
    pub fn new(http_client_conf: Arc<HttpClientConf>, conf: SinkConf) -> Sink {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();
        let client = build_http_client(&http_client_conf);
        let join_handle_data = JoinHandleData {
            stop_signal_rx,
            http_client_conf,
            conf,
            client,
            mb_rx,
        };
        let join_handle = Self::event_loop(join_handle_data);

        Sink {
            stop_signal_tx,
            join_handle: Some(join_handle),
            mb_tx,
        }
    }

    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn update_conf(&mut self, _old_conf: SinkConf, new_conf: SinkConf) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = new_conf;
        let join_handle = Self::event_loop(join_handle_data);
        self.join_handle = Some(join_handle);
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        let schema = match join_handle_data.http_client_conf.ssl_enable {
            true => "https",
            false => "http",
        };
        let url = format!(
            "{}://{}:{}{}",
            schema,
            &join_handle_data.http_client_conf.host,
            join_handle_data.http_client_conf.port,
            &join_handle_data.conf.path
        );
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    mb = join_handle_data.mb_rx.recv() => {
                        match mb {
                            Some(mb) => Sink::send_request(&join_handle_data.client, &url, &join_handle_data.http_client_conf, &join_handle_data.conf, mb).await,
                            None => warn!("http客户端收到空消息"),
                        }
                    }
                }
            }
        })
    }

    async fn send_request(
        client: &Client,
        url: &String,
        http_client_conf: &Arc<HttpClientConf>,
        conf: &SinkConf,
        _mb: RuleMessageBatch,
    ) {
        // todo 重复使用

        let mut builder = match conf.method {
            types::apps::http_client::SinkMethod::Get => client.get(url),
            types::apps::http_client::SinkMethod::Post => client.post(url),
            types::apps::http_client::SinkMethod::Delete => client.delete(url),
            types::apps::http_client::SinkMethod::Patch => client.patch(url),
            types::apps::http_client::SinkMethod::Put => client.put(url),
            types::apps::http_client::SinkMethod::Head => client.head(url),
        };

        builder = builder.query(&conf.query_params);

        builder = build_headers(builder, &conf.headers, &http_client_conf.headers);

        let request = builder.build().unwrap();

        match client.execute(request).await {
            Ok(_) => trace!("http client send ok"),
            Err(e) => warn!("http client send err:{:?}", e),
        }
    }

    pub async fn update_http_client(&mut self, http_client_conf: Arc<HttpClientConf>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.http_client_conf = http_client_conf;
        let join_handle = Self::event_loop(join_handle_data);
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }
}
