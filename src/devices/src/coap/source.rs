use anyhow::Result;
use common::{del_mb_rx, error::HaliaResult, get_id, get_mb_rx, persistence, ref_info::RefInfo};
use message::MessageBatch;
use protocol::coap::{
    client::{ObserveMessage, UdpCoAPClient},
    request::Packet,
};
use tokio::sync::{broadcast, oneshot};
use tracing::warn;
use types::{
    devices::coap::{CoapConf, SourceConf},
    BaseConf, CreateUpdateSourceOrSinkReq, SearchSourcesOrSinksItemResp,
};
use uuid::Uuid;

pub struct Source {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: SourceConf,

    observe_tx: Option<oneshot::Sender<ObserveMessage>>,

    pub ref_info: RefInfo,
    mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl Source {
    pub async fn new(
        device_id: &Uuid,
        source_id: Option<Uuid>,
        req: CreateUpdateSourceOrSinkReq,
    ) -> HaliaResult<Self> {
        let (base_conf, ext_conf, data) = Self::parse_conf(req)?;

        let (source_id, new) = get_id(source_id);
        if new {
            persistence::create_source(device_id, &source_id, &data).await?;
        }

        Ok(Self {
            id: source_id,
            base_conf,
            ext_conf,
            observe_tx: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
        })
    }

    fn parse_conf(req: CreateUpdateSourceOrSinkReq) -> HaliaResult<(BaseConf, SourceConf, String)> {
        let data = serde_json::to_string(&req)?;
        let conf: SourceConf = serde_json::from_value(req.ext)?;
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
        persistence::devices::coap::delete_observe(device_id, &self.id).await?;
        Ok(())
    }

    pub async fn start(&mut self, conf: &CoapConf) -> Result<()> {
        let client = UdpCoAPClient::new_udp((conf.host.clone(), conf.port)).await?;

        let (mb_tx, _) = broadcast::channel(16);

        let observe_mb_tx = mb_tx.clone();
        self.mb_tx = Some(mb_tx);

        let observe_tx = client
            .observe(&self.ext_conf.path, move |msg| {
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
            .observe(&self.ext_conf.path, move |msg| {
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
