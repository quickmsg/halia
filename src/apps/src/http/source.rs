use std::{sync::Arc, time::Duration};

use common::error::HaliaResult;
use log::warn;
use message::{MessageBatch, RuleMessageBatch};
use reqwest::{Client, Request};
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
    time,
};
use types::apps::http_client::{HttpClientConf, SourceConf};

use super::{build_basic_auth, build_headers};

pub struct Source {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
}

pub struct JoinHandleData {
    pub stop_signal_rx: watch::Receiver<()>,
    pub http_client_conf: Arc<HttpClientConf>,
    pub conf: SourceConf,
    pub client: Client,
    pub mb_txs: Vec<UnboundedSender<RuleMessageBatch>>,
}

impl Source {
    pub async fn new(http_client_conf: Arc<HttpClientConf>, conf: SourceConf) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());

        let http_client = Client::new();
        let join_handle_data = JoinHandleData {
            stop_signal_rx,
            http_client_conf,
            conf,
            client: http_client.clone(),
            mb_txs: vec![],
        };
        let join_handle = Self::event_loop(join_handle_data).await;
        Self {
            stop_signal_tx,
            join_handle: Some(join_handle),
        }
    }

    pub fn validate_conf(_conf: &SourceConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn update_conf(&mut self, _old_conf: SourceConf, new_conf: SourceConf) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = new_conf;
        let join_handle = Self::event_loop(join_handle_data).await;
        self.join_handle = Some(join_handle);
    }

    pub async fn update_http_client(&mut self, http_client_conf: Arc<HttpClientConf>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.http_client_conf = http_client_conf;
        let join_hdnale = Self::event_loop(join_handle_data).await;
        self.join_handle = Some(join_hdnale);
    }

    async fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            let mut interval =
                time::interval(Duration::from_millis(join_handle_data.conf.interval));
            let mut builder = join_handle_data.client.get(format!(
                "{}:{}{}",
                &join_handle_data.http_client_conf.host,
                &join_handle_data.http_client_conf.port,
                join_handle_data.conf.path
            ));
            builder = build_basic_auth(
                builder,
                &join_handle_data.conf.basic_auth,
                &join_handle_data.http_client_conf.basic_auth,
            );
            builder = build_headers(
                builder,
                &join_handle_data.conf.headers,
                &join_handle_data.http_client_conf.headers,
            );
            builder = builder.query(&join_handle_data.conf.query_params);
            let request = builder.build().unwrap();

            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    _ = interval.tick() => {
                        Self::do_request(&join_handle_data.client, request.try_clone().unwrap(), &mut join_handle_data.mb_txs).await;
                    }
                }
            }
        })
    }

    async fn do_request(
        client: &Client,
        request: Request,
        mb_txs: &mut Vec<UnboundedSender<RuleMessageBatch>>,
    ) {
        if mb_txs.len() == 0 {
            return;
        }

        match client.execute(request).await {
            Ok(resp) => {
                if resp.status().is_success() {
                    match resp.bytes().await {
                        Ok(body) => match MessageBatch::from_json(body) {
                            Ok(mb) => {
                                if mb_txs.len() == 1 {
                                    let rmb = RuleMessageBatch::Owned(mb);
                                    if let Err(_) = mb_txs[0].send(rmb) {
                                        mb_txs.remove(0);
                                    }
                                } else {
                                    let rmb = RuleMessageBatch::Arc(Arc::new(mb));
                                    mb_txs.retain(|tx| tx.send(rmb.clone()).is_ok());
                                }
                            }
                            Err(e) => warn!("{}", e),
                        },

                        Err(e) => warn!("{}", e),
                    }
                } else {
                    warn!("请求失败，状态码：{}", resp.status());
                }
            }
            Err(e) => warn!("{}", e),
        }
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn get_rx(&mut self) -> UnboundedReceiver<RuleMessageBatch> {
        let mut join_handle_data = self.stop().await;
        let (tx, rx) = unbounded_channel();
        join_handle_data.mb_txs.push(tx);
        rx
    }
}
