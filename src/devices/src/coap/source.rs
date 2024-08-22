use std::time::Duration;

use anyhow::Result;
use common::{
    error::{HaliaError, HaliaResult},
    persistence,
    ref_info::RefInfo,
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
    devices::coap::{CoapConf, SourceConf, SourceMethod},
    BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksItemResp,
};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: SourceConf,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<JoinHandle<(UdpCoAPClient, mpsc::Receiver<()>)>>,

    observe_tx: Option<oneshot::Sender<ObserveMessage>>,

    pub ref_info: RefInfo,
    mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Source {
    pub async fn new(
        device_id: &Uuid,
        source_id: Uuid,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<Self> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;

        Ok(Self {
            id: source_id,
            base_conf,
            ext_conf,
            observe_tx: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
            stop_signal_tx: None,
            join_handle: None,
        })
    }

    fn parse_conf(req: CreateUpdateSourceOrSinkReq) -> HaliaResult<(BaseConf, SourceConf, String)> {
        let data = serde_json::to_string(&req)?;
        let conf: SourceConf = serde_json::from_value(req.ext)?;
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
        // TODO check

        Ok((req.base, conf, data))
    }

    // pub fn check_duplicate(&self, req: &CreateUpdateSourceReq) -> HaliaResult<()> {
    //     if self.base_conf.name == req.base.name {
    //         return Err(HaliaError::NameExists);
    //     }

    //     Ok(())
    // }

    pub fn search(&self) -> SearchSourcesOrSinksItemResp {
        SearchSourcesOrSinksItemResp {
            id: self.id.clone(),
            conf: CreateUpdateSourceOrSinkReq {
                base: self.base_conf.clone(),
                ext: serde_json::to_value(self.ext_conf.clone()).unwrap(),
            },
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        req: CreateUpdateSourceOrSinkReq,
        coap_conf: &CoapConf,
    ) -> HaliaResult<()> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        persistence::update_source(device_id, &self.id, &data).await?;

        if restart {
            _ = self.restart(coap_conf).await;
        }

        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        persistence::delete_source(device_id, &self.id).await?;
        Ok(())
    }

    pub async fn start(&mut self, conf: &CoapConf) -> Result<()> {
        let client = UdpCoAPClient::new_udp((conf.host.clone(), conf.port)).await?;

        let (mb_tx, _) = broadcast::channel(16);

        match self.ext_conf.method {
            SourceMethod::Get => todo!(),
            SourceMethod::Observe => self.start_observe(client, mb_tx.clone()).await?,
        }

        self.mb_tx = Some(mb_tx);

        Ok(())
    }

    async fn start_observe(
        &mut self,
        client: UdpCoAPClient,
        observe_mb_tx: broadcast::Sender<MessageBatch>,
    ) -> Result<()> {
        let observe_tx = client
            .observe(
                &self.ext_conf.observe_conf.as_ref().unwrap().path,
                move |msg| Self::observe_handler(msg, &observe_mb_tx),
            )
            .await?;
        self.observe_tx = Some(observe_tx);

        Ok(())
    }

    async fn start_api(&mut self, client: UdpCoAPClient) -> HaliaResult<()> {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        if let Err(e) = self.event_loop(client, stop_signal_rx).await {
            return Err(HaliaError::Common(e.to_string()));
        }

        Ok(())
    }

    async fn event_loop(
        &mut self,
        client: UdpCoAPClient,
        mut stop_signal_rx: mpsc::Receiver<()>,
    ) -> Result<()> {
        // let options = transform_options(&self.ext_conf.observe_conf.as_ref().unwrap().options)?;
        let request =
            RequestBuilder::new(&self.ext_conf.get_conf.as_ref().unwrap().path, Method::Get)
                // .domain(self.conf.ext.domain.clone())
                // .options(options)
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
        // match String::from_utf8(msg.payload) {
        //     Ok(data) => debug!("{}", data),
        //     Err(e) => warn!("{}", e),
        // }
        // debug!("receive msg :{:?}", msg.payload);
    }

    pub async fn stop(&mut self) {
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

    pub async fn restart(&mut self, coap_conf: &CoapConf) -> Result<()> {
        if let Err(e) = self
            .observe_tx
            .take()
            .unwrap()
            .send(ObserveMessage::Terminate)
        {
            warn!("stop send msg err:{:?}", e);
        }

        // let observe_mb_tx = self.mb_tx.as_ref().unwrap().clone();
        // let client = UdpCoAPClient::new_udp((coap_conf.host.clone(), coap_conf.port)).await?;
        // let observe_tx = client
        //     .observe(&self.ext_conf.path, move |msg| {
        //         Self::observe_handler(msg, &observe_mb_tx)
        //     })
        //     .await?;
        // self.observe_tx = Some(observe_tx);

        Ok(())
    }

    pub fn get_mb_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        todo!()
    }

    pub fn del_mb_rx(&mut self, rule_id: &Uuid) {
        todo!()
    }
}
