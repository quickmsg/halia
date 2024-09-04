use std::{sync::Arc, time::Duration};

use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_info_resp,
};
use message::MessageBatch;
use reqwest::Client;
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};
use tracing::{trace, warn};
use types::{
    apps::http_client::{HttpClientConf, SourceConf},
    BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,
    base_conf: BaseConf,
    ext_conf: Arc<SourceConf>,

    pub mb_tx: Option<broadcast::Sender<MessageBatch>>,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<JoinHandle<(mpsc::Receiver<()>, Arc<HttpClientConf>, Client)>>,
}

impl Source {
    pub fn new(id: Uuid, base_conf: BaseConf, ext_conf: SourceConf) -> HaliaResult<Self> {
        Self::validate_conf(&ext_conf)?;
        Ok(Self {
            id,
            base_conf,
            ext_conf: Arc::new(ext_conf),
            stop_signal_tx: None,
            join_handle: None,
            mb_tx: None,
        })
    }

    fn validate_conf(_conf: &SourceConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, ext_conf: &SourceConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        if self.ext_conf.path == ext_conf.path
            && self.ext_conf.query_params == ext_conf.query_params
        {
            return Err(HaliaError::AddressExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        get_search_sources_or_sinks_info_resp!(self)
    }

    pub async fn update_conf(
        &mut self,
        base_conf: BaseConf,
        ext_conf: SourceConf,
    ) -> HaliaResult<()> {
        Self::validate_conf(&ext_conf)?;

        self.base_conf = base_conf;
        if *self.ext_conf == ext_conf {
            return Ok(());
        }
        self.ext_conf = Arc::new(ext_conf);

        match &self.stop_signal_tx {
            Some(stop_signal_tx) => {
                _ = stop_signal_tx.send(()).await;
                let (stop_signal_rx, http_client_conf, client) =
                    self.join_handle.take().unwrap().await.unwrap();
                self.event_loop(http_client_conf, stop_signal_rx, client)
                    .await;
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub async fn update_http_client(&mut self, http_client_conf: Arc<HttpClientConf>) {
        self.stop().await;
        let (stop_signal_rx, _, client) = self.join_handle.take().unwrap().await.unwrap();
        self.event_loop(http_client_conf, stop_signal_rx, client)
            .await;
    }

    pub async fn start(&mut self, http_client_conf: Arc<HttpClientConf>) {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, _) = broadcast::channel(16);
        self.mb_tx = Some(mb_tx);

        self.event_loop(http_client_conf, stop_signal_rx, reqwest::Client::new())
            .await;
    }

    async fn event_loop(
        &mut self,
        http_client_conf: Arc<HttpClientConf>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        client: Client,
    ) {
        let interval = self.ext_conf.interval;
        let ext_conf = self.ext_conf.clone();

        let join_handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(interval));

            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return(stop_signal_rx, http_client_conf, client);
                    }

                    _ = interval.tick() => {
                        Self::send_request(&client, &http_client_conf, &ext_conf).await;
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    async fn send_request(
        client: &Client,
        http_client_conf: &Arc<HttpClientConf>,
        ext_conf: &Arc<SourceConf>,
    ) {
        let mut builder = client.get(format!("{}{}", &http_client_conf.host, ext_conf.path));
        if let Some(basic_auth) = &ext_conf.basic_auth {
            builder = builder.basic_auth(basic_auth.username.clone(), basic_auth.password.clone());
        }

        for (key, value) in ext_conf.headers.iter() {
            builder = builder.header(key, value);
        }
        builder = builder.query(&ext_conf.query_params);

        for (k, v) in ext_conf.headers.iter() {
            builder = builder.header(k, v);
        }
        let request = builder.build().unwrap();
        match client.execute(request).await {
            Ok(resp) => trace!("{:?}", resp),
            Err(e) => warn!("{}", e),
        }
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
