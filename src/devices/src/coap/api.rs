use std::time::Duration;

use anyhow::Result;
use common::{
    del_mb_rx,
    error::{HaliaError, HaliaResult},
    get_id, get_mb_rx, persistence,
    ref_info::RefInfo,
};
use message::MessageBatch;
use protocol::coap::{
    client::UdpCoAPClient,
    request::{Method, RequestBuilder},
};
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle,
    time,
};
use tracing::debug;
use types::devices::coap::{CoapConf, CreateUpdateAPIReq, SearchAPIsItemResp};
use uuid::Uuid;

use super::transform_options;

pub struct API {
    pub id: Uuid,
    conf: CreateUpdateAPIReq,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<JoinHandle<(UdpCoAPClient, mpsc::Receiver<()>)>>,

    pub ref_info: RefInfo,
    mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl API {
    pub async fn new(
        device_id: &Uuid,
        api_id: Option<Uuid>,
        req: CreateUpdateAPIReq,
    ) -> HaliaResult<Self> {
        Self::check_conf(&req)?;

        let (api_id, new) = get_id(api_id);
        if new {
            // persistence::devices::coap::create_api(
            //     device_id,
            //     &api_id,
            //     serde_json::to_string(&req).unwrap(),
            // )
            // .await?;
        }

        Ok(Self {
            id: api_id,
            conf: req,
            stop_signal_tx: None,
            join_handle: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
        })
    }

    fn check_conf(_req: &CreateUpdateAPIReq) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, req: &CreateUpdateAPIReq) -> HaliaResult<()> {
        if self.conf.base.name == req.base.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchAPIsItemResp {
        SearchAPIsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
            rule_ref: self.ref_info.get_rule_ref(),
        }
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateAPIReq) -> HaliaResult<()> {
        Self::check_conf(&req)?;

        // persistence::devices::coap::update_api(
        //     device_id,
        //     &self.id,
        //     serde_json::to_string(&req).unwrap(),
        // )
        // .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;

        if restart {
            match &self.stop_signal_tx {
                Some(stop_signal_tx) => {
                    stop_signal_tx.send(()).await.unwrap();
                    let (client, stop_signal_rx) = self.join_handle.take().unwrap().await.unwrap();
                    _ = self.event_loop(client, stop_signal_rx).await;
                }
                None => {}
            }
        }

        Ok(())
    }

    pub async fn delete(&self, device_id: &Uuid) -> HaliaResult<()> {
        if !self.ref_info.can_delete() {
            return Err(HaliaError::DeleteRefing);
        }

        // persistence::devices::coap::delete_api(device_id, &self.id).await?;

        match &self.stop_signal_tx {
            Some(stop_signal_tx) => stop_signal_tx.send(()).await.unwrap(),
            None => {}
        }

        Ok(())
    }

    pub async fn start(&mut self, conf: &CoapConf) -> HaliaResult<()> {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let client = UdpCoAPClient::new_udp((conf.host.clone(), conf.port)).await?;
        if let Err(e) = self.event_loop(client, stop_signal_rx).await {
            return Err(HaliaError::Common(e.to_string()));
        }

        Ok(())
    }

    pub async fn restart(&mut self, conf: &CoapConf) -> HaliaResult<()> {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        let (_, stop_signal_rx) = self.join_handle.take().unwrap().await.unwrap();
        let client = UdpCoAPClient::new_udp((conf.host.clone(), conf.port)).await?;
        if let Err(e) = self.event_loop(client, stop_signal_rx).await {
            return Err(HaliaError::Common(e.to_string()));
        }

        Ok(())
    }

    pub async fn stop(&mut self) -> HaliaResult<()> {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;

        Ok(())
    }

    async fn event_loop(
        &mut self,
        client: UdpCoAPClient,
        mut stop_signal_rx: mpsc::Receiver<()>,
    ) -> Result<()> {
        let options = transform_options(&self.conf.ext.options)?;
        let request = RequestBuilder::new(&self.conf.ext.path, Method::Get)
            .domain(self.conf.ext.domain.clone())
            .options(options)
            .build();
        let mut interval = time::interval(Duration::from_millis(self.conf.ext.interval));
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

    // async fn read() {}

    pub fn get_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        get_mb_rx!(self, rule_id)
    }

    pub fn del_rx(&mut self, rule_id: &Uuid) {
        del_mb_rx!(self, rule_id)
    }
}
