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
use tokio::sync::{broadcast, oneshot};
use tracing::debug;
use types::devices::coap::{CoapConf, CreateUpdateObserveReq, SearchObservesItemResp};
use uuid::Uuid;

pub struct Observe {
    pub id: Uuid,
    conf: CreateUpdateObserveReq,

    on: bool,

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
            on: false,
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
    ) -> HaliaResult<()> {
        Self::check_conf(&req)?;

        persistence::devices::coap::update_observe(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;
        todo!()
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::coap::delete_observe(device_id, &self.id).await?;
        Ok(())
    }

    pub async fn start(&mut self, conf: &CoapConf) -> Result<()> {
        let client = UdpCoAPClient::new_udp((conf.host.clone(), conf.port)).await?;
        let observe_tx = client
            .observe(&self.conf.ext.path, Self::observe_handler)
            .await?;
        self.observe_tx = Some(observe_tx);

        Ok(())
    }

    fn observe_handler(msg: Packet) {
        debug!("receive msg :{:?}", msg);
    }

    pub async fn stop(&mut self) {
        _ = self
            .observe_tx
            .take()
            .unwrap()
            .send(ObserveMessage::Terminate);
    }

    pub async fn restart(&mut self) {

    }
}
