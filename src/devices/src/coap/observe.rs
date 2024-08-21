use anyhow::Result;
use common::{
    del_mb_rx,
    error::{HaliaError, HaliaResult},
    get_id, get_mb_rx, persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use protocol::coap::{
    client::{ObserveMessage, UdpCoAPClient},
    request::Packet,
};
use tokio::sync::{broadcast, oneshot};
use tracing::warn;
use types::devices::coap::{CoapConf, CreateUpdateObserveReq, SearchObservesItemResp};
use uuid::Uuid;

pub struct Observe {
    pub id: Uuid,
    conf: CreateUpdateObserveReq,

    observe_tx: Option<oneshot::Sender<ObserveMessage>>,

    pub ref_info: RefInfo,
    mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Observe {
    pub async fn new(
        device_id: &Uuid,
        observe_id: Option<Uuid>,
        req: CreateUpdateObserveReq,
    ) -> HaliaResult<Self> {
        Self::check_conf(&req)?;

        let (observe_id, new) = get_id(observe_id);
        if new {
            // persistence::devices::coap::create_observe(
            //     device_id,
            //     &observe_id,
            //     serde_json::to_string(&req).unwrap(),
            // )
            // .await?;
        }

        Ok(Self {
            id: observe_id,
            conf: req,
            observe_tx: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
        })
    }

    fn check_conf(req: &CreateUpdateObserveReq) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateObserveReq) -> HaliaResult<()> {
        if self.conf.base.name == req.base.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchObservesItemResp {
        SearchObservesItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn update(
        &mut self,
        device_id: &Uuid,
        req: CreateUpdateObserveReq,
        coap_conf: &CoapConf,
    ) -> HaliaResult<()> {
        Self::check_conf(&req)?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;

        // persistence::devices::coap::update_observe(
        //     device_id,
        //     &self.id,
        //     serde_json::to_string(&self.conf).unwrap(),
        // )
        // .await?;

        if restart {
            _ = self.restart(coap_conf).await;
        }

        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        // persistence::devices::coap::delete_observe(device_id, &self.id).await?;
        Ok(())
    }

    pub async fn start(&mut self, conf: &CoapConf) -> Result<()> {
        let client = UdpCoAPClient::new_udp((conf.host.clone(), conf.port)).await?;

        let (mb_tx, _) = broadcast::channel(16);

        let observe_mb_tx = mb_tx.clone();
        self.mb_tx = Some(mb_tx);

        let observe_tx = client
            .observe(&self.conf.ext.path, move |msg| {
                Self::observe_handler(msg, &observe_mb_tx)
            })
            .await?;
        self.observe_tx = Some(observe_tx);

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

        let observe_mb_tx = self.mb_tx.as_ref().unwrap().clone();
        let client = UdpCoAPClient::new_udp((coap_conf.host.clone(), coap_conf.port)).await?;
        let observe_tx = client
            .observe(&self.conf.ext.path, move |msg| {
                Self::observe_handler(msg, &observe_mb_tx)
            })
            .await?;
        self.observe_tx = Some(observe_tx);

        Ok(())
    }

    pub fn get_mb_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        get_mb_rx!(self, rule_id)
    }

    pub fn del_mb_rx(&mut self, rule_id: &Uuid) {
        del_mb_rx!(self, rule_id);
    }
}
