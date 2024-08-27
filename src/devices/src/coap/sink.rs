use anyhow::Result;
use common::error::{HaliaError, HaliaResult};
use message::MessageBatch;
use protocol::coap::{
    client::UdpCoAPClient,
    request::{Method, RequestBuilder},
};
use tokio::{select, sync::mpsc, task::JoinHandle};
use types::{
    devices::coap::{CoapConf, SinkConf},
    BaseConf, SearchSourcesOrSinksInfoResp,
};
use uuid::Uuid;

use super::transform_options;

pub struct Sink {
    pub id: Uuid,

    base_conf: BaseConf,
    ext_conf: SinkConf,

    stop_signal_tx: Option<mpsc::Sender<()>>,
    pub mb_tx: Option<mpsc::Sender<MessageBatch>>,

    join_handle: Option<
        JoinHandle<(
            UdpCoAPClient,
            mpsc::Receiver<MessageBatch>,
            mpsc::Receiver<()>,
        )>,
    >,
}

impl Sink {
    pub fn new(id: Uuid, base_conf: BaseConf, ext_conf: SinkConf) -> HaliaResult<Self> {
        Self::validate_conf(&ext_conf)?;

        Ok(Self {
            id,
            base_conf,
            ext_conf,
            stop_signal_tx: None,
            mb_tx: None,
            join_handle: None,
        })
    }

    fn validate_conf(_ext_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub fn check_duplicate(&self, base_conf: &BaseConf, _ext_conf: &SinkConf) -> HaliaResult<()> {
        if self.base_conf.name == base_conf.name {
            return Err(HaliaError::NameExists);
        }

        Ok(())
    }

    pub fn search(&self) -> SearchSourcesOrSinksInfoResp {
        todo!()
        // SearchSinksItemResp {
        //     id: self.id.clone(),
        //     conf: self.conf.clone(),
        //     rule_ref: self.ref_info.get_rule_ref(),
        // }
    }

    pub async fn update(&mut self, base_conf: BaseConf, ext_conf: SinkConf) -> HaliaResult<()> {
        Self::validate_conf(&ext_conf)?;

        let mut restart = false;
        if self.ext_conf != ext_conf {
            restart = true;
        }
        self.base_conf = base_conf;
        self.ext_conf = ext_conf;

        if restart {
            self.stop_signal_tx
                .as_ref()
                .unwrap()
                .send(())
                .await
                .unwrap();

            let (client, mb_rx, stop_signal_rx) = self.join_handle.take().unwrap().await.unwrap();
            _ = self.event_loop(stop_signal_rx, mb_rx, client).await;
        }

        Ok(())
    }

    pub async fn start(&mut self, coap_conf: &CoapConf) -> HaliaResult<()> {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        self.stop_signal_tx = Some(stop_signal_tx);

        let (mb_tx, mb_rx) = mpsc::channel(16);
        self.mb_tx = Some(mb_tx);

        let client = UdpCoAPClient::new_udp((coap_conf.host.clone(), coap_conf.port)).await?;
        _ = self.event_loop(stop_signal_rx, mb_rx, client).await;

        Ok(())
    }

    pub async fn restart(&mut self, coap_conf: &CoapConf) -> HaliaResult<()> {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();

        let (_, mb_rx, stop_signal_rx) = self.join_handle.take().unwrap().await.unwrap();
        let client = UdpCoAPClient::new_udp((coap_conf.host.clone(), coap_conf.port)).await?;
        _ = self.event_loop(stop_signal_rx, mb_rx, client).await;

        Ok(())
    }

    async fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut publish_rx: mpsc::Receiver<MessageBatch>,
        client: UdpCoAPClient,
    ) -> Result<()> {
        let method = match &self.ext_conf.method {
            types::devices::coap::SinkMethod::Post => Method::Post,
            types::devices::coap::SinkMethod::Put => Method::Put,
            types::devices::coap::SinkMethod::Delete => Method::Delete,
        };

        // 在check conf中进行options校验
        let options = transform_options(&self.ext_conf.options).unwrap();
        let request = RequestBuilder::new(&self.ext_conf.path, method)
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
                        if let Some(_mb) = mb {
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

    pub async fn stop(&mut self) {
        self.stop_signal_tx
            .as_ref()
            .unwrap()
            .send(())
            .await
            .unwrap();
        self.stop_signal_tx = None;
    }
}