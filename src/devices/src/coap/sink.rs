use anyhow::Result;
use common::{
    check_and_set_on_false, check_and_set_on_true, error::HaliaResult, get_id, persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use protocol::coap::{
    client::UdpCoAPClient,
    request::{Method, RequestBuilder},
};
use tokio::{select, sync::mpsc, task::JoinHandle};
use types::devices::coap::{CoapConf, CreateUpdateSinkReq, SearchSinksItemResp};
use uuid::Uuid;

use super::transform_options;

pub struct Sink {
    pub id: Uuid,
    pub conf: CreateUpdateSinkReq,

    on: bool,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    publish_tx: Option<mpsc::Sender<MessageBatch>>,

    join_handle: Option<
        JoinHandle<(
            UdpCoAPClient,
            mpsc::Receiver<MessageBatch>,
            mpsc::Receiver<()>,
        )>,
    >,

    pub ref_info: RefInfo,
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
            on: false,
            stop_signal_tx: None,
            publish_tx: None,
            ref_info: RefInfo::new(),
            join_handle: None,
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

    pub async fn start(&mut self, coap_conf: &CoapConf) -> HaliaResult<()> {
        check_and_set_on_true!(self);

        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);
        let (publish_tx, publish_rx) = mpsc::channel(16);
        self.publish_tx = Some(publish_tx);

        let client = UdpCoAPClient::new_udp((coap_conf.host.clone(), coap_conf.port)).await?;
        self.event_loop(stop_signal_rx, publish_rx, client).await;

        Ok(())
    }

    pub async fn restart(&mut self, coap_conf: &CoapConf) -> HaliaResult<()> {
        todo!()
    }

    async fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut publish_rx: mpsc::Receiver<MessageBatch>,
        client: UdpCoAPClient,
    ) -> Result<()> {
        let method = match &self.conf.ext.method {
            types::devices::coap::SinkMethod::Post => Method::Post,
            types::devices::coap::SinkMethod::Put => Method::Put,
            types::devices::coap::SinkMethod::Delete => Method::Delete,
        };

        let options = transform_options(&self.conf.ext.options)?;
        let request = RequestBuilder::new(&self.conf.ext.path, method)
            .options(options)
            // .domain(coap_conf.domain.clone())
            .build();

        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (client, publish_rx, stop_signal_rx)
                    }

                    mb = publish_rx.recv() => {
                        if let Some(mb) = mb {
                            match client.send(request.clone()).await {
                                Ok(_) => {}
                                Err(_) => {}
                            }
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        check_and_set_on_false!(self);

        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;

        Ok(())
    }

    pub async fn delete(&mut self, device_id: &Uuid) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(common::error::HaliaError::Common(
                "引用中，不能被删除".to_owned(),
            ));
        }
        persistence::devices::coap::delete_sink(device_id, &self.id).await?;
        Ok(())
    }

    pub fn get_mb_tx(&mut self, rule_id: &Uuid) -> mpsc::Sender<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        self.publish_tx.as_ref().unwrap().clone()
    }

    pub fn del_mb_tx(&mut self, rule_id: &Uuid) {
        self.ref_info.deactive_ref(rule_id);
    }
}
