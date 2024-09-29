use std::io::Result as IoResult;
use std::{sync::Arc, time::Duration};

use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use protocol::coap::{
    client::{ObserveMessage, UdpCoAPClient},
    request::{Method, RequestBuilder},
};
use tokio::{
    select,
    sync::{broadcast, mpsc, oneshot, Mutex},
    task::JoinHandle,
    time,
};
use tracing::{debug, warn};
use types::devices::coap::{GetConf, ObserveConf, SourceConf, SourceMethod};
use url::form_urlencoded;

use super::{transform_options, TokenManager};

pub struct Source {
    // for api
    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<
        JoinHandle<(
            Arc<UdpCoAPClient>,
            mpsc::Receiver<()>,
            Arc<Mutex<TokenManager>>,
            GetConf,
        )>,
    >,

    // for observe
    oberserve_conf: Option<ObserveConf>,
    observe_tx: Option<oneshot::Sender<ObserveMessage>>,
    coap_client: Option<Arc<UdpCoAPClient>>,
    token_manager: Option<Arc<Mutex<TokenManager>>>,
    token: Option<Vec<u8>>,

    pub mb_tx: broadcast::Sender<MessageBatch>,
}

impl Source {
    pub fn validate_conf(conf: SourceConf) -> HaliaResult<()> {
        match conf.method {
            SourceMethod::Get => {
                if conf.get.is_none() {
                    return Err(HaliaError::Common("get请求为空！".to_owned()));
                }
            }
            SourceMethod::Observe => {
                if conf.observe.is_none() {
                    return Err(HaliaError::Common("observe配置为空！".to_owned()));
                }
            }
        }

        Ok(())
    }

    pub async fn new(
        conf: SourceConf,
        coap_client: Arc<UdpCoAPClient>,
        token_manager: Arc<Mutex<TokenManager>>,
    ) -> Self {
        let (mb_tx, _) = broadcast::channel(16);

        let mut source = Self {
            stop_signal_tx: None,
            join_handle: None,

            oberserve_conf: None,
            observe_tx: None,
            coap_client: None,
            token_manager: None,
            token: None,

            mb_tx,
        };

        match &conf.method {
            SourceMethod::Get => {
                let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
                source.stop_signal_tx = Some(stop_signal_tx);
                let join_handle = Self::start_get(
                    conf.get.unwrap(),
                    coap_client,
                    stop_signal_rx,
                    token_manager,
                )
                .await;
                source.join_handle = Some(join_handle);
            }
            SourceMethod::Observe => {
                let observe_conf = conf.observe.unwrap();
                let token = token_manager.lock().await.acquire();
                Self::start_observe(
                    &coap_client,
                    &observe_conf,
                    source.mb_tx.clone(),
                    token.clone(),
                )
                .await;
                source.coap_client = Some(coap_client);
                source.oberserve_conf = Some(observe_conf);
                source.token_manager = Some(token_manager);
                source.token = Some(token);
            }
        }

        source
    }

    pub async fn update_conf(
        &mut self,
        old_conf: SourceConf,
        new_conf: SourceConf,
    ) -> HaliaResult<()> {
        match (&old_conf.method, &new_conf.method) {
            (SourceMethod::Get, SourceMethod::Get) => {
                let (coap_client, stop_signal_rx, token_manager, _) = self.stop_get().await;
                let join_handle = Self::start_get(
                    new_conf.get.unwrap(),
                    coap_client,
                    stop_signal_rx,
                    token_manager,
                )
                .await;
                self.join_handle = Some(join_handle);
            }
            (SourceMethod::Get, SourceMethod::Observe) => {
                let (coap_client, _, token_manager, _) = self.stop_get().await;
                self.stop_signal_tx = None;
                self.join_handle = None;

                let observe_conf = new_conf.observe.unwrap();
                let token = token_manager.lock().await.acquire();
                match Self::start_observe(
                    &coap_client,
                    &observe_conf,
                    self.mb_tx.clone(),
                    token.clone(),
                )
                .await
                {
                    Ok(observe_tx) => self.observe_tx = Some(observe_tx),
                    Err(e) => {
                        warn!("start observe err:{:?}", e);
                    }
                }
                self.oberserve_conf = Some(observe_conf);
                self.coap_client = Some(coap_client);
                self.token_manager = Some(token_manager);
                self.token = Some(token);
            }
            (SourceMethod::Observe, SourceMethod::Get) => {
                self.stop_obeserve().await;
                self.observe_tx = None;
                let token_manager = self.token_manager.take().unwrap();
                token_manager
                    .lock()
                    .await
                    .release(self.token.take().unwrap());

                let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
                self.stop_signal_tx = Some(stop_signal_tx);
                let coap_client = self.coap_client.take().unwrap();
                let join_handle = Self::start_get(
                    new_conf.get.unwrap(),
                    coap_client,
                    stop_signal_rx,
                    token_manager,
                )
                .await;
                self.join_handle = Some(join_handle);
            }
            (SourceMethod::Observe, SourceMethod::Observe) => {
                self.stop_obeserve().await;
                let observe_conf = new_conf.observe.unwrap();
                Self::start_observe(
                    self.coap_client.as_ref().unwrap(),
                    &observe_conf,
                    self.mb_tx.clone(),
                    self.token.as_ref().unwrap().clone(),
                )
                .await;
                self.oberserve_conf = Some(observe_conf);
            }
        }

        Ok(())
    }

    pub async fn update_coap_client(&mut self, coap_client: Arc<UdpCoAPClient>) -> HaliaResult<()> {
        match self.stop_signal_tx.is_some() {
            true => {
                let (coap_client, stop_signal_rx, token_manager, conf) = self.stop_get().await;
                let join_handle =
                    Self::start_get(conf, coap_client, stop_signal_rx, token_manager).await;
                self.join_handle = Some(join_handle);
            }
            false => {
                self.stop_obeserve().await;
                match Self::start_observe(
                    self.coap_client.as_ref().unwrap(),
                    self.oberserve_conf.as_ref().unwrap(),
                    self.mb_tx.clone(),
                    self.token.as_ref().unwrap().clone(),
                )
                .await
                {
                    Ok(observe_tx) => self.observe_tx = Some(observe_tx),
                    Err(e) => warn!("start observe err:{:?}", e),
                }
            }
        }

        Ok(())
    }

    async fn start_get(
        conf: GetConf,
        coap_client: Arc<UdpCoAPClient>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        token_manager: Arc<Mutex<TokenManager>>,
    ) -> JoinHandle<(
        Arc<UdpCoAPClient>,
        mpsc::Receiver<()>,
        Arc<Mutex<TokenManager>>,
        GetConf,
    )> {
        let mut interval = time::interval(Duration::from_millis(conf.interval));
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (coap_client, stop_signal_rx, token_manager, conf);
                    }

                    _ = interval.tick() => {
                        Self::coap_get(&coap_client, &token_manager, &conf).await;
                    }
                }
            }
        })
    }

    async fn coap_get(
        coap_client: &Arc<UdpCoAPClient>,
        token_manager: &Arc<Mutex<TokenManager>>,
        get_conf: &GetConf,
    ) {
        let mut request_builder = RequestBuilder::new(&get_conf.path, Method::Get);

        if get_conf.querys.len() > 0 {
            let encoded_params: String = form_urlencoded::Serializer::new(String::new())
                .extend_pairs(get_conf.querys.clone())
                .finish();
            request_builder = request_builder.queries(Some(encoded_params.into_bytes()));
        }

        if get_conf.options.len() > 0 {
            let options = transform_options(&get_conf.options).unwrap();
            request_builder = request_builder.options(options);
        }

        let token = token_manager.lock().await.acquire();
        let request = request_builder.token(Some(token.clone())).build();
        match coap_client.send(request).await {
            Ok(_) => debug!("success"),
            Err(e) => warn!("{:?}", e),
        }
        token_manager.lock().await.release(token);
    }

    async fn start_observe(
        coap_client: &Arc<UdpCoAPClient>,
        conf: &ObserveConf,
        mb_tx: broadcast::Sender<MessageBatch>,
        token: Vec<u8>,
    ) -> IoResult<oneshot::Sender<ObserveMessage>> {
        let request_builder = RequestBuilder::new(&conf.path, Method::Get).token(Some(token));
        let request = request_builder.build();

        // 加入重试功能
        coap_client
            .observe_with(request, move |msg| {
                debug!("{:?}", msg);
                if mb_tx.receiver_count() > 0 {
                    _ = mb_tx.send(MessageBatch::from_json(msg.payload.into()).unwrap());
                }
            })
            .await
    }

    pub async fn stop(&mut self) {
        // TODO
        self.observe_tx = None;
    }

    async fn stop_get(
        &mut self,
    ) -> (
        Arc<UdpCoAPClient>,
        mpsc::Receiver<()>,
        Arc<Mutex<TokenManager>>,
        GetConf,
    ) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    async fn stop_obeserve(&mut self) {
        if let Err(e) = self
            .observe_tx
            .take()
            .unwrap()
            .send(ObserveMessage::Terminate)
        {
            warn!("stop send msg err:{:?}", e);
        }

        self.token_manager
            .as_ref()
            .unwrap()
            .lock()
            .await
            .release(self.token.take().unwrap());
    }
}
