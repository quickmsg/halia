use std::{sync::Arc, time::Duration};

use anyhow::Result;
use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_info_resp,
};
use message::MessageBatch;
use protocol::coap::{
    client::{ObserveMessage, UdpCoAPClient},
    request::{Method, RequestBuilder},
};
use tokio::{
    select,
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tracing::{debug, warn};
use types::{
    devices::coap::{SourceConf, SourceMethod},
    BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksInfoResp,
};
use url::form_urlencoded;
use uuid::Uuid;

use super::transform_options;

pub struct Source {
    pub id: Uuid,
    base_conf: BaseConf,
    ext_conf: SourceConf,

    on: bool,

    // for api
    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<JoinHandle<(Arc<UdpCoAPClient>, mpsc::Receiver<()>)>>,

    // for observe
    observe_tx: Option<oneshot::Sender<ObserveMessage>>,
    coap_client: Option<Arc<UdpCoAPClient>>,

    pub mb_tx: Option<broadcast::Sender<MessageBatch>>,
}
impl Source {
    pub fn new(id: Uuid, base_conf: BaseConf, ext_conf: SourceConf) -> HaliaResult<Self> {
        Self::validate_conf(&ext_conf)?;

        Ok(Self {
            id,
            base_conf,
            ext_conf,
            on: false,
            stop_signal_tx: None,
            join_handle: None,
            observe_tx: None,
            coap_client: None,
            mb_tx: None,
        })
    }

    fn validate_conf(conf: &SourceConf) -> HaliaResult<()> {
        debug!("{:?}", conf);
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

    pub fn check_duplicate(&self, base_conf: &BaseConf, _ext_conf: &SourceConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
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
        if self.ext_conf == ext_conf {
            return Ok(());
        }

        if !self.on {
            self.ext_conf = ext_conf;
            return Ok(());
        }

        match (&self.ext_conf.method, &ext_conf.method) {
            (SourceMethod::Get, SourceMethod::Get) => {
                self.ext_conf = ext_conf;
                self.stop_get().await;
                let (coap_client, stop_signal_rx) = self.join_handle.take().unwrap().await.unwrap();
                self.start_get(coap_client, stop_signal_rx).await;
            }
            (SourceMethod::Get, SourceMethod::Observe) => {
                self.ext_conf = ext_conf;
                self.stop_get().await;
                let (coap_client, _) = self.join_handle.take().unwrap().await.unwrap();
                self.stop_signal_tx = None;
                self.join_handle = None;

                self.coap_client = Some(coap_client);
                self.start_observe().await;
            }
            (SourceMethod::Observe, SourceMethod::Get) => {
                self.ext_conf = ext_conf;
                self.stop_obeserve();
                self.observe_tx = None;
                let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
                self.stop_signal_tx = Some(stop_signal_tx);
                let coap_client = self.coap_client.take().unwrap();
                self.start_get(coap_client, stop_signal_rx).await;
            }
            (SourceMethod::Observe, SourceMethod::Observe) => {
                self.ext_conf = ext_conf;
                self.stop_obeserve();
                self.start_observe().await;
            }
        }

        Ok(())
    }

    pub async fn update_coap_client(&mut self, coap_client: Arc<UdpCoAPClient>) -> HaliaResult<()> {
        match &self.ext_conf.method {
            SourceMethod::Get => {
                self.stop_get().await;
                let (_, stop_signal_rx) = self.join_handle.take().unwrap().await.unwrap();
                self.start_get(coap_client, stop_signal_rx).await;
            }
            SourceMethod::Observe => {
                self.stop_obeserve();
                self.coap_client = Some(coap_client);
                self.start_observe().await;
            }
        }

        Ok(())
    }

    pub async fn start(&mut self, coap_client: Arc<UdpCoAPClient>) -> Result<()> {
        self.on = true;

        let (mb_tx, _) = broadcast::channel(16);
        self.mb_tx = Some(mb_tx);

        match &self.ext_conf.method {
            SourceMethod::Get => {
                let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
                self.stop_signal_tx = Some(stop_signal_tx);
                self.start_get(coap_client, stop_signal_rx).await;
            }
            SourceMethod::Observe => {
                self.coap_client = Some(coap_client);
                self.start_observe().await;
            }
        }

        Ok(())
    }

    async fn start_get(
        &mut self,
        coap_client: Arc<UdpCoAPClient>,
        mut stop_signal_rx: mpsc::Receiver<()>,
    ) {
        let get_conf = self.ext_conf.get.as_ref().unwrap();
        let mut request_builder = RequestBuilder::new(&get_conf.path, Method::Get);

        let encoded_params: String = form_urlencoded::Serializer::new(String::new())
            .extend_pairs(get_conf.querys.clone())
            .finish();
        request_builder = request_builder.queries(Some(encoded_params.into_bytes()));

        let options = transform_options(&get_conf.options).unwrap();
        let request = request_builder
            .options(options)
            // .token(token)
            .build();
        let mut interval = time::interval(Duration::from_millis(get_conf.interval));
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (coap_client, stop_signal_rx)
                    }

                    _ = interval.tick() => {
                        match coap_client.send(request.clone()).await {
                            Ok(resp) => debug!("{:?}", resp),
                            Err(e) => debug!("{:?}", e),
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    async fn start_observe(&mut self) {
        let mb_tx = self.mb_tx.as_ref().unwrap().clone();
        match self
            .coap_client
            .as_ref()
            .unwrap()
            .observe(
                &self.ext_conf.observe.as_ref().unwrap().path,
                // move |msg| Self::observe_handler(msg, &mb_tx),
                move |msg| {
                    debug!("{:?}", msg);
                    if mb_tx.receiver_count() > 0 {
                        _ = mb_tx.send(MessageBatch::from_json(msg.payload.into()).unwrap());
                    }
                },
            )
            .await
        {
            Ok(observe_tx) => self.observe_tx = Some(observe_tx),
            Err(e) => warn!("{:?}", e),
        }
    }

    pub async fn stop(&mut self) {
        self.on = false;

        self.observe_tx = None;
        self.mb_tx = None;
    }

    async fn stop_get(&mut self) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
    }

    fn stop_obeserve(&mut self) {
        if let Err(e) = self
            .observe_tx
            .take()
            .unwrap()
            .send(ObserveMessage::Terminate)
        {
            warn!("stop send msg err:{:?}", e);
        }
    }
}
