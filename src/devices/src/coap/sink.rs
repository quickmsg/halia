use std::sync::Arc;

use coap_protocol::{
    client::UdpCoAPClient,
    request::{Method, RequestBuilder},
};
use common::error::HaliaResult;
use message::MessageBatch;
use tokio::{
    select,
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use types::devices::device::coap::SinkConf;

use super::{transform_options, TokenManager};

pub struct Sink {
    stop_signal_tx: mpsc::Sender<()>,

    join_handle: Option<
        JoinHandle<(
            Arc<UdpCoAPClient>,
            mpsc::Receiver<MessageBatch>,
            mpsc::Receiver<()>,
            SinkConf,
        )>,
    >,

    token_manager: Arc<Mutex<TokenManager>>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Sink {
    pub fn validate_conf(_conf: SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn new(
        coap_client: Arc<UdpCoAPClient>,
        conf: SinkConf,
        token_manager: Arc<Mutex<TokenManager>>,
    ) -> Self {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let join_handle = Self::event_loop(coap_client, conf, stop_signal_rx, mb_rx).await;

        Self {
            stop_signal_tx,
            mb_tx,
            join_handle: Some(join_handle),
            token_manager,
        }
    }

    pub async fn update_conf(&mut self, _old_conf: SinkConf, new_conf: SinkConf) {
        let (coap_client, mb_rx, stop_signal_rx, _) = self.stop().await;
        let join_handle = Self::event_loop(coap_client, new_conf, stop_signal_rx, mb_rx).await;
        self.join_handle = Some(join_handle);
    }

    pub async fn update_coap_client(&mut self, coap_client: Arc<UdpCoAPClient>) {
        let (_, mb_rx, stop_signal_rx, conf) = self.stop().await;
        let join_handle = Self::event_loop(coap_client, conf, stop_signal_rx, mb_rx).await;
        self.join_handle = Some(join_handle);
    }

    async fn event_loop(
        coap_client: Arc<UdpCoAPClient>,
        conf: SinkConf,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) -> JoinHandle<(
        Arc<UdpCoAPClient>,
        mpsc::Receiver<MessageBatch>,
        mpsc::Receiver<()>,
        SinkConf,
    )> {
        let method = match &conf.method {
            types::devices::device::coap::SinkMethod::Post => Method::Post,
            types::devices::device::coap::SinkMethod::Put => Method::Put,
            types::devices::device::coap::SinkMethod::Delete => Method::Delete,
        };

        // 在check conf中进行options校验
        let options = transform_options(&conf.options).unwrap();
        let request = RequestBuilder::new(&conf.path, method)
            .options(options)
            // .domain(coap_conf.domain.clone())
            .build();

        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (coap_client, mb_rx, stop_signal_rx, conf)
                    }

                    mb = mb_rx.recv() => {
                        if let Some(_mb) = mb {
                            match coap_client.send(request.clone()).await {
                                Ok(_) => {}
                                Err(_) => {}
                            }
                        }
                    }
                }
            }
        })
    }

    pub async fn stop(
        &mut self,
    ) -> (
        Arc<UdpCoAPClient>,
        mpsc::Receiver<MessageBatch>,
        mpsc::Receiver<()>,
        SinkConf,
    ) {
        self.stop_signal_tx.send(()).await.unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }
}
