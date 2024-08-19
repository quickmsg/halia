use std::sync::Arc;

use anyhow::Result;
use common::{
    error::{HaliaError, HaliaResult},
    get_id, persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use protocol::coap::{
    client::{ObserveMessage, UdpCoAPClient},
    request::Packet,
};
use tokio::sync::{broadcast, oneshot, RwLock};
use tracing::{debug, warn};
use types::devices::coap::{CoapConf, CreateUpdateObserveReq, SearchObservesItemResp};
use uuid::Uuid;

pub struct Observe {
    pub id: Uuid,
    conf: CreateUpdateObserveReq,

    observe_tx: Option<oneshot::Sender<ObserveMessage>>,

    pub ref_info: RefInfo,
    mb_tx: Arc<RwLock<Option<broadcast::Sender<MessageBatch>>>>,
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
            persistence::devices::coap::create_observe(
                device_id,
                &observe_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: observe_id,
            conf: req,
            observe_tx: None,
            ref_info: RefInfo::new(),
            mb_tx: Arc::new(RwLock::new(None)),
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
    ) -> HaliaResult<()> {
        Self::check_conf(&req)?;

        persistence::devices::coap::update_observe(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::coap::delete_observe(device_id, &self.id).await?;
        Ok(())
    }

    pub async fn start(&mut self, conf: &CoapConf) -> Result<()> {
        let client = UdpCoAPClient::new_udp((conf.host.clone(), conf.port)).await?;

        let mb_tx = self.mb_tx.clone();

        let observe_tx = client
            .observe(&self.conf.ext.path, move |msg| {
                Self::observe_handler(msg, &mb_tx)
            })
            .await?;
        self.observe_tx = Some(observe_tx);

        Ok(())
    }

    fn observe_handler(msg: Packet, mb_tx: &Arc<RwLock<Option<broadcast::Sender<MessageBatch>>>>) {
        // match mb_tx.write().await {
        //     Some(_) => todo!(),
        //     None => todo!(),
        // }
        match String::from_utf8(msg.payload) {
            Ok(data) => debug!("{}", data),
            Err(e) => warn!("{}", e),
        }
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
    }

    pub async fn restart(&mut self) {}

    pub async fn get_mb_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        self.ref_info.active_ref(rule_id);

        if let Some(tx) = self.mb_tx.read().await.as_ref() {
            tx.subscribe()
        } else {
            let (sender, receiver) = broadcast::channel(16);
            *self.mb_tx.write().await = Some(sender);
            receiver
        }
    }
}
