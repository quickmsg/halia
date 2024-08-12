use anyhow::Result;
use std::time::Duration;

use common::{
    check_and_set_on_false, check_and_set_on_true,
    error::{HaliaError, HaliaResult},
    get_id, persistence,
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

    on: bool,
    stop_signal_tx: Option<mpsc::Sender<()>>,
    join_handle: Option<JoinHandle<(UdpCoAPClient, mpsc::Receiver<()>)>>,

    ref_info: RefInfo,
    mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl API {
    pub async fn new(
        device_id: &Uuid,
        api_id: Option<Uuid>,
        req: CreateUpdateAPIReq,
    ) -> HaliaResult<Self> {
        let (api_id, new) = get_id(api_id);

        if new {
            persistence::devices::coap::create_api(
                device_id,
                &api_id,
                serde_json::to_string(&req).unwrap(),
            )
            .await?;
        }

        Ok(Self {
            id: api_id,
            conf: req,
            on: false,
            stop_signal_tx: None,
            join_handle: None,
            ref_info: RefInfo::new(),
            mb_tx: None,
        })
    }

    pub fn search(&self) -> SearchAPIsItemResp {
        SearchAPIsItemResp {
            id: self.id.clone(),
            conf: self.conf.clone(),
        }
    }

    pub async fn update(&mut self, device_id: &Uuid, req: CreateUpdateAPIReq) -> HaliaResult<()> {
        persistence::devices::coap::update_api(
            device_id,
            &self.id,
            serde_json::to_string(&req).unwrap(),
        )
        .await?;

        let mut restart = false;
        if self.conf.ext != req.ext {
            restart = true;
        }
        self.conf = req;
        if self.on && restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();
            let (client, stop_signal_rx) = self.join_handle.take().unwrap().await.unwrap();
            self.event_loop(client, stop_signal_rx).await;
        }

        Ok(())
    }

    pub async fn delete(&self, device_id: &Uuid) -> HaliaResult<()> {
        persistence::devices::coap::delete_api(device_id, &self.id).await?;
        if self.on {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();
        }
        Ok(())
    }

    pub async fn start(&mut self, conf: &CoapConf) -> HaliaResult<()> {
        check_and_set_on_true!(self);

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

    async fn read() {}

    pub fn add_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.add_ref(rule_id);
    }

    pub fn get_mb_rx(&mut self, rule_id: &Uuid) -> broadcast::Receiver<MessageBatch> {
        self.ref_info.active_ref(rule_id);
        match &self.mb_tx {
            Some(tx) => tx.subscribe(),
            None => {
                let (tx, rx) = broadcast::channel(16);
                self.mb_tx = Some(tx);
                rx
            }
        }
    }

    pub fn del_ref(&mut self, rule_id: &Uuid) {
        self.ref_info.del_ref(rule_id);
    }

    pub fn del_mb_rx(&mut self, rule_id: &Uuid) {
        self.ref_info.deactive_ref(rule_id);
        if self.ref_info.can_stop() {
            self.mb_tx = None;
        }
    }

    pub fn can_stop(&self) -> bool {
        self.ref_info.can_delete()
    }

    pub fn can_delete(&self) -> bool {
        self.ref_info.can_delete()
    }
}