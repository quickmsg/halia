use std::{sync::Arc, time::Duration};

use anyhow::Result;
use common::{
    error::{HaliaError, HaliaResult},
    get_search_sources_or_sinks_info_resp,
};
use message::MessageBatch;
use protocol::coap::{
    client::{ObserveMessage, UdpCoAPClient},
    request::{Method, Packet, RequestBuilder},
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
            observe_tx: None,
            mb_tx: None,
            stop_signal_tx: None,
            join_handle: None,
        })
    }

    pub fn validate_conf(conf: &SourceConf) -> HaliaResult<()> {
        match conf.method {
            SourceMethod::Get => {
                if conf.get_conf.is_none() {
                    return Err(HaliaError::Common("get请求为空！".to_owned()));
                }
            }
            SourceMethod::Observe => {
                if conf.observe_conf.is_none() {
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

        let mut restart = false;
        let method = self.ext_conf.method.clone();
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        if self.on && restart {
            if method == SourceMethod::Observe {
                if let Err(e) = self
                    .observe_tx
                    .take()
                    .unwrap()
                    .send(ObserveMessage::Terminate)
                {
                    warn!("stop send msg err:{:?}", e);
                }
            }

            _ = self.restart().await;
        }

        Ok(())
    }

    pub async fn update_coap_client(&mut self, coap_client: Arc<UdpCoAPClient>) -> HaliaResult<()> {
        match &self.observe_tx {
            Some(observe_tx) => {
                if let Err(e) = self
                    .observe_tx
                    .take()
                    .unwrap()
                    .send(ObserveMessage::Terminate)
                {
                    warn!("stop send msg err:{:?}", e);
                }
            }
            None => {}
        }
        Ok(())
    }

    pub async fn start(&mut self, client: Arc<UdpCoAPClient>) -> Result<()> {
        self.on = true;
        let (mb_tx, _) = broadcast::channel(16);

        match self.ext_conf.method {
            SourceMethod::Get => self.start_api(client).await?,
            SourceMethod::Observe => self.start_observe(client, mb_tx.clone()).await?,
        }

        self.mb_tx = Some(mb_tx);

        Ok(())
    }

    async fn start_observe(
        &mut self,
        client: Arc<UdpCoAPClient>,
        mb_tx: broadcast::Sender<MessageBatch>,
    ) -> Result<()> {
        let observe_tx = client
            .observe(
                &self.ext_conf.observe_conf.as_ref().unwrap().path,
                move |msg| Self::observe_handler(msg, &mb_tx),
            )
            .await?;
        self.observe_tx = Some(observe_tx);

        Ok(())
    }

    async fn start_api(&mut self, client: Arc<UdpCoAPClient>) -> HaliaResult<()> {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        self.event_loop(client, stop_signal_rx).await?;

        Ok(())
    }

    async fn event_loop(
        &mut self,
        client: Arc<UdpCoAPClient>,
        mut stop_signal_rx: mpsc::Receiver<()>,
    ) -> Result<()> {
        let mut request_builder =
            RequestBuilder::new(&self.ext_conf.get_conf.as_ref().unwrap().path, Method::Get);

        if let Some(querys) = &self.ext_conf.get_conf.as_ref().unwrap().querys {
            let encoded_params: String = form_urlencoded::Serializer::new(String::new())
                .extend_pairs(querys)
                .finish();
            request_builder = request_builder.queries(Some(encoded_params.into_bytes()));
        }

        let options = transform_options(&self.ext_conf.observe_conf.as_ref().unwrap().options)?;
        let request = request_builder
            // .domain(self.conf.ext.domain.clone())
            .options(options)
            .build();
        let mut interval = time::interval(Duration::from_millis(
            self.ext_conf.get_conf.as_ref().unwrap().interval,
        ));
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (client, stop_signal_rx)
                    }

                    _ = interval.tick() => {
                        match client.send(request.clone()).await {
                            Ok(resp) => debug!("{:?}", resp),
                            Err(e) => debug!("{:?}", e),
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
        Ok(())
    }

    fn observe_handler(msg: Packet, mb_tx: &broadcast::Sender<MessageBatch>) {
        if mb_tx.receiver_count() > 0 {
            _ = mb_tx.send(MessageBatch::from_json(msg.payload.into()).unwrap());
        }
    }

    pub async fn stop(&mut self) {
        self.on = false;

        if let Err(e) = self
            .observe_tx
            .take()
            .unwrap()
            .send(ObserveMessage::Terminate)
        {
            warn!("stop send msg err:{:?}", e);
        }
        self.observe_tx = None;
        self.mb_tx = None;
    }

    pub async fn restart(&mut self) -> Result<()> {
        if let Err(e) = self
            .observe_tx
            .take()
            .unwrap()
            .send(ObserveMessage::Terminate)
        {
            warn!("stop send msg err:{:?}", e);
        }

        _ = self.stop_signal_tx.as_ref().unwrap().send(()).await;
        let (coap_client, stop_signal_rx) = self.join_handle.take().unwrap().await.unwrap();

        match self.ext_conf.method {
            SourceMethod::Get => self.start_api(coap_client).await?,
            SourceMethod::Observe => {
                let (mb_tx, _) = broadcast::channel(16);
                self.start_observe(coap_client, mb_tx.clone()).await?;
            }
        }

        Ok(())
    }
}
