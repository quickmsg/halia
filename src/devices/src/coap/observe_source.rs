use std::sync::Arc;

use anyhow::Result;
use common::error::HaliaResult;
use message::MessageBatch;
use protocol::coap::{
    client::{ObserveMessage, UdpCoAPClient},
    request::Packet,
};
use tokio::sync::{broadcast, oneshot};
use tracing::warn;
use types::devices::coap::ObserveConf;

use super::source::SourceItem;

pub struct ObserveSource {
    on: bool,
    conf: ObserveConf,
    observe_tx: Option<oneshot::Sender<ObserveMessage>>,

    pub mb_tx: Option<broadcast::Sender<MessageBatch>>,
}

impl ObserveSource {
    pub fn new(conf: ObserveConf) -> HaliaResult<SourceItem> {
        Self::validate_conf(&conf)?;

        Ok(SourceItem::Observe(Self {
            on: false,
            conf,
            observe_tx: None,
            mb_tx: None,
        }))
    }

    fn validate_conf(_conf: &ObserveConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn update_conf(&mut self, conf: ObserveConf) -> HaliaResult<()> {
        Self::validate_conf(&conf)?;

        let mut restart = false;
        if self.conf != conf {
            restart = true;
        }

        if self.on && restart {
            if let Err(e) = self
                .observe_tx
                .take()
                .unwrap()
                .send(ObserveMessage::Terminate)
            {
                warn!("stop send msg err:{:?}", e);
            }

            // _ = self.restart().await;
        }

        Ok(())
    }

    pub async fn update_coap_client(&mut self, coap_client: Arc<UdpCoAPClient>) -> HaliaResult<()> {
        match &self.observe_tx {
            Some(observe_tx) => {
                if let Err(e) = self
                    .observe_tx
                    .take()
                    .unwrap()
                    .send(ObserveMessage::Terminate)
                {
                    warn!("stop send msg err:{:?}", e);
                }
            }
            None => {}
        }
        Ok(())
    }

    pub async fn start(&mut self, client: Arc<UdpCoAPClient>) -> Result<()> {
        self.on = true;
        let (mb_tx, _) = broadcast::channel(16);
        let observe_mb_tx = mb_tx.clone();
        self.mb_tx = Some(mb_tx);

        let observe_tx = client
            .observe(&self.conf.path, move |msg| {
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
    }

    pub async fn stop(&mut self) {
        self.on = false;
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

    // pub async fn restart(&mut self) -> Result<()> {
    //     if let Err(e) = self
    //         .observe_tx
    //         .take()
    //         .unwrap()
    //         .send(ObserveMessage::Terminate)
    //     {
    //         warn!("stop send msg err:{:?}", e);
    //     }

    //     _ = self.stop_signal_tx.as_ref().unwrap().send(()).await;
    //     let (coap_client, stop_signal_rx) = self.join_handle.take().unwrap().await.unwrap();

    //     match self.ext_conf.method {
    //         SourceMethod::Get => self.start_api(coap_client).await?,
    //         SourceMethod::Observe => {
    //             let (mb_tx, _) = broadcast::channel(16);
    //             self.start_observe(coap_client, mb_tx.clone()).await?;
    //         }
    //     }

    //     Ok(())
    // }
}
