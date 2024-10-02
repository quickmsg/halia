use std::{sync::Arc, time::Duration};

use common::error::HaliaResult;
use message::MessageBatch;
use reqwest::Client;
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};
use tracing::{trace, warn};
use types::apps::http_client::{HttpClientConf, SourceConf};

use super::build_headers;

pub struct Source {
    stop_signal_tx: mpsc::Sender<()>,
    join_handle: Option<JoinHandle<(mpsc::Receiver<()>, Arc<HttpClientConf>, SourceConf, Client)>>,
    pub mb_tx: broadcast::Sender<MessageBatch>,
}

impl Source {
    pub async fn new(http_client_conf: Arc<HttpClientConf>, conf: SourceConf) -> Self {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        let (mb_tx, _) = broadcast::channel(16);
        let http_client = Client::new();
        let join_handle =
            Self::event_loop(http_client_conf, conf, stop_signal_rx, http_client).await;
        Self {
            stop_signal_tx,
            join_handle: Some(join_handle),
            mb_tx,
        }
    }

    pub fn validate_conf(_conf: &SourceConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn update_conf(&mut self, _old_conf: SourceConf, new_conf: SourceConf) {
        let (stop_signal_rx, http_client_conf, _, client) = self.stop().await;
        let join_handle =
            Self::event_loop(http_client_conf, new_conf, stop_signal_rx, client).await;
        self.join_handle = Some(join_handle);
    }

    pub async fn update_http_client(&mut self, http_client_conf: Arc<HttpClientConf>) {
        let (stop_signal_rx, _, conf, client) = self.stop().await;
        let join_hdnale = Self::event_loop(http_client_conf, conf, stop_signal_rx, client).await;
        self.join_handle = Some(join_hdnale);
    }

    async fn event_loop(
        http_client_conf: Arc<HttpClientConf>,
        conf: SourceConf,
        mut stop_signal_rx: mpsc::Receiver<()>,
        client: Client,
    ) -> JoinHandle<(mpsc::Receiver<()>, Arc<HttpClientConf>, SourceConf, Client)> {
        let interval = conf.interval;
        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));

            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return(stop_signal_rx, http_client_conf, conf, client);
                    }

                    _ = interval.tick() => {
                        Self::send_request(&client, &http_client_conf, &conf).await;
                    }
                }
            }
        })
    }

    async fn send_request(
        client: &Client,
        http_client_conf: &Arc<HttpClientConf>,
        conf: &SourceConf,
    ) {
        let mut builder = client.get(format!("{}{}", &http_client_conf.host, conf.path));
        match (&conf.basic_auth, &http_client_conf.basic_auth) {
            (None, None) => {}
            (None, Some(basic_auth)) => {
                builder =
                    builder.basic_auth(basic_auth.username.clone(), basic_auth.password.clone());
            }
            (Some(basic_auth), None) => {
                builder =
                    builder.basic_auth(basic_auth.username.clone(), basic_auth.password.clone());
            }
            (Some(basic_auth), Some(_)) => {
                builder =
                    builder.basic_auth(basic_auth.username.clone(), basic_auth.password.clone());
            }
        }

        builder = build_headers(builder, &conf.headers, &http_client_conf.headers);

        builder = builder.query(&conf.query_params);

        // for (k, v) in ext_conf.headers.iter() {
        //     builder = builder.header(k, v);
        // }
        let request = builder.build().unwrap();
        match client.execute(request).await {
            Ok(resp) => trace!("{:?}", resp),
            Err(e) => warn!("{}", e),
        }
    }

    pub async fn stop(&mut self) -> (mpsc::Receiver<()>, Arc<HttpClientConf>, SourceConf, Client) {
        self.stop_signal_tx.send(()).await.unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }
}
