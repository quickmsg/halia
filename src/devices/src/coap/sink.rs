use common::{error::HaliaResult, get_id, persistence, ref_info::RefInfo};
use message::MessageBatch;
use protocol::coap::request::{Method, RequestBuilder};
use tokio::{select, sync::mpsc};
use types::devices::coap::{CoapConf, CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

pub struct Sink {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    publish_tx: Option<mpsc::Sender<MessageBatch>>,

    ref_info: RefInfo,
}

impl Sink {
    pub async fn new(
        device_id: &Uuid,
        sink_id: Option<Uuid>,
        req: CreateUpdateSinkReq,
    ) -> HaliaResult<Self> {
        let (sink_id, new) = get_id(sink_id);

        if new {
            persistence::devices::coap::create_sink(
                device_id,
                &sink_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: sink_id,
            conf: req,
            stop_signal_tx: None,
            publish_tx: None,
            ref_info: RefInfo::new(),
        })
    }

    pub fn search(&self) -> SearchSinksItemResp {
        SearchSinksItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateSinkReq) -> HaliaResult<()> {
        persistence::devices::coap::update_sink(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        self.conf = req;

        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::coap::delete_sink(device_id, &self.id).await?;
        todo!()
    }

    pub async fn start(&mut self, coap_conf: &CoapConf) {
        let (stop_signal_tx, mut stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (publish_tx, mut publish_rx) = mpsc::channel(16);
        self.publish_tx = Some(publish_tx);

        let method = match &self.conf.ext.method {
            types::devices::coap::SinkMethod::Get => todo!(),
            types::devices::coap::SinkMethod::Post => todo!(),
            types::devices::coap::SinkMethod::Put => todo!(),
            types::devices::coap::SinkMethod::Delete => todo!(),
            // &"POST" => Method::Post,
            // &"PUT" => Method::Put,
            // &"DELETE" => Method::Delete,
            // _ => unreachable!(),
        };

        let request = RequestBuilder::new(&self.conf.path, method)
            .domain(self.conf.domain.clone())
            .build();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return
                    }

                    mb = publish_rx.recv() => {
                        if let Some(mb) = mb {
                            match coap_client.send(request.clone()).await {
                                Ok(_) => todo!(),
                                Err(_) => todo!(),
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;
    }

    pub fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.add_ref(rule_id);
    }

    pub fn get_mb_tx(&mut self, rule_id: &Uuid) -> mpsc::Sender<MessageBatch> {
        todo!()
    }

    pub fn del_mb_tx(&mut self, rule_id: &Uuid) {
        todo!()
    }

    pub fn del_ref(&mut self, rule_id: &Uuid) {
        todo!()
    }

    pub fn can_stop(&self) -> bool {
        self.ref_info.can_stop()
    }

    pub fn can_delete(&self) -> bool {
        self.ref_info.can_delete()
    }
}
