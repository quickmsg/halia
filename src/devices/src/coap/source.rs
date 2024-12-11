use std::io::Result as IoResult;
use std::{sync::Arc, time::Duration};

use coap_protocol::{
    client::{ObserveMessage, UdpCoAPClient},
    request::{Method, RequestBuilder},
};
use common::error::{HaliaError, HaliaResult};
use futures::lock::BiLock;
use halia_derive::{ResourceErr, SourceRxs};
use message::{MessageBatch, RuleMessageBatch};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch;
use tokio::{
    select,
    sync::{broadcast, mpsc, oneshot, Mutex},
    task::JoinHandle,
    time,
};
use tracing::{debug, warn};
use types::devices::device::coap::{SourceConf, SourceMethod};
use url::form_urlencoded;

use super::{transform_options, TokenManager};

#[derive(ResourceErr, SourceRxs)]
pub struct Source {
    join_handle: Option<JoinHandle<TaskLoop>>,

    get_stop_signal_tx: Option<mpsc::Sender<()>>,
    observe_stop_signal_tx: Option<oneshot::Sender<ObserveMessage>>,

    err: BiLock<Option<Arc<String>>>,

    mb_txs: BiLock<Vec<UnboundedSender<RuleMessageBatch>>>,
}

pub struct TaskLoop {
    source_conf: SourceConf,
    coap_client: Arc<UdpCoAPClient>,
    stop_signal_rx: watch::Receiver<()>,
    token_manager: Arc<Mutex<TokenManager>>,
}

impl TaskLoop {
    fn new(
        source_conf: SourceConf,
        coap_client: Arc<UdpCoAPClient>,
        err: BiLock<Option<Arc<String>>>,
        mb_txs: BiLock<Vec<UnboundedSender<RuleMessageBatch>>>,
        token_manager: Arc<Mutex<TokenManager>>,
    ) -> Self {
        todo!()
    }

    fn start(
        mut self,
    ) -> (
        JoinHandle<Self>,
        Option<mpsc::Sender<()>>,
        Option<oneshot::Sender<ObserveMessage>>,
    ) {
        match self.source_conf.method {
            SourceMethod::Get => todo!(),
            SourceMethod::Observe => todo!(),
        }
        todo!()
    }

    fn start_get(mut self) -> JoinHandle<Self> {
        let mut interval = time::interval(Duration::from_millis(
            self.source_conf.get.as_ref().unwrap().interval,
        ));
        tokio::spawn(async move {
            loop {
                select! {
                    _ = self.stop_signal_rx.changed() => {
                        return self;
                    }

                    _ = interval.tick() => {
                        self.coap_get().await;
                    }
                }
            }
        })
    }

    async fn coap_get(&self) {
        let mut request_builder = RequestBuilder::new(&self.source_conf.path, Method::Get);

        if let Some(querys) = &self.source_conf.querys {
            let encoded_params: String = form_urlencoded::Serializer::new(String::new())
                .extend_pairs(querys.clone())
                .finish();
            request_builder = request_builder.queries(Some(encoded_params.into_bytes()));
        }

        if let Some(options) = &self.source_conf.options {
            let options = transform_options(options).unwrap();
            request_builder = request_builder.options(options);
        }

        let token = self.token_manager.lock().await.acquire();
        let request = request_builder.token(Some(token.clone())).build();
        match self.coap_client.send(request).await {
            Ok(_) => debug!("success"),
            Err(e) => warn!("{:?}", e),
        }
        self.token_manager.lock().await.release(token);
    }

    async fn start_observe(
        &self,
        mb_tx: broadcast::Sender<MessageBatch>,
        token: Vec<u8>,
    ) -> IoResult<oneshot::Sender<ObserveMessage>> {
        let request_builder =
            RequestBuilder::new(&self.source_conf.path, Method::Get).token(Some(token));
        let request = request_builder.build();

        // 加入重试功能
        self.coap_client
            .observe_with(request, move |msg| {
                debug!("{:?}", msg);
                if mb_tx.receiver_count() > 0 {
                    _ = mb_tx.send(MessageBatch::from_json(msg.payload.into()).unwrap());
                }
            })
            .await
    }
}

impl Source {
    pub fn validate_conf(conf: SourceConf) -> HaliaResult<()> {
        match conf.method {
            SourceMethod::Get => {
                if conf.get.is_none() {
                    return Err(HaliaError::Common("get请求为空！".to_owned()));
                }
            }
            SourceMethod::Observe => {
                if conf.observe.is_none() {
                    return Err(HaliaError::Common("observe配置为空！".to_owned()));
                }
            }
        }

        Ok(())
    }

    pub async fn new(
        source_conf: SourceConf,
        coap_client: Arc<UdpCoAPClient>,
        token_manager: Arc<Mutex<TokenManager>>,
    ) -> Self {
        let (err1, err2) = BiLock::new(None);
        let (mb_txs1, mb_txs2) = BiLock::new(vec![]);

        let task_loop = TaskLoop::new(source_conf, coap_client, err1, mb_txs1, token_manager);
        let (join_handle, get_stop_signal_tx, observe_stop_signal_tx) = task_loop.start();

        Source {
            join_handle: Some(join_handle),
            get_stop_signal_tx,
            observe_stop_signal_tx,
            mb_txs: mb_txs2,
            err: err2,
        }
    }

    pub async fn update_conf(&mut self, conf: SourceConf) -> HaliaResult<()> {
        // match (&old_conf.method, &new_conf.method) {
        //     (SourceMethod::Get, SourceMethod::Get) => {
        //         let (coap_client, stop_signal_rx, token_manager, _) = self.stop_get().await;
        //         let join_handle = Self::start_get(
        //             new_conf.get.unwrap(),
        //             coap_client,
        //             stop_signal_rx,
        //             token_manager,
        //         )
        //         .await;
        //         self.join_handle = Some(join_handle);
        //     }
        //     (SourceMethod::Get, SourceMethod::Observe) => {
        //         let (coap_client, _, token_manager, _) = self.stop_get().await;
        //         self.stop_signal_tx = None;
        //         self.join_handle = None;

        //         let observe_conf = new_conf.observe.unwrap();
        //         let token = token_manager.lock().await.acquire();
        //         match Self::start_observe(
        //             &coap_client,
        //             &observe_conf,
        //             self.mb_tx.clone(),
        //             token.clone(),
        //         )
        //         .await
        //         {
        //             Ok(observe_tx) => self.observe_tx = Some(observe_tx),
        //             Err(e) => {
        //                 warn!("start observe err:{:?}", e);
        //             }
        //         }
        //         self.oberserve_conf = Some(observe_conf);
        //         self.coap_client = Some(coap_client);
        //         self.token_manager = Some(token_manager);
        //         self.token = Some(token);
        //     }
        //     (SourceMethod::Observe, SourceMethod::Get) => {
        //         self.stop_obeserve().await;
        //         self.observe_tx = None;
        //         let token_manager = self.token_manager.take().unwrap();
        //         token_manager
        //             .lock()
        //             .await
        //             .release(self.token.take().unwrap());

        //         let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        //         self.stop_signal_tx = Some(stop_signal_tx);
        //         let coap_client = self.coap_client.take().unwrap();
        //         let join_handle = Self::start_get(
        //             new_conf.get.unwrap(),
        //             coap_client,
        //             stop_signal_rx,
        //             token_manager,
        //         )
        //         .await;
        //         self.join_handle = Some(join_handle);
        //     }
        //     (SourceMethod::Observe, SourceMethod::Observe) => {
        //         self.stop_obeserve().await;
        //         let observe_conf = new_conf.observe.unwrap();
        //         Self::start_observe(
        //             self.coap_client.as_ref().unwrap(),
        //             &observe_conf,
        //             self.mb_tx.clone(),
        //             self.token.as_ref().unwrap().clone(),
        //         )
        //         .await;
        //         self.oberserve_conf = Some(observe_conf);
        //     }
        // }
        todo!()

        // Ok(())
    }

    pub async fn update_coap_client(&mut self, coap_client: Arc<UdpCoAPClient>) -> HaliaResult<()> {
        let mut task_loop = self.stop().await;
        task_loop.coap_client = coap_client;
        let (join_handle, _, _) = task_loop.start();
        self.join_handle = Some(join_handle);

        Ok(())
    }

    pub async fn stop(&mut self) -> TaskLoop {
        if let Some(stop_tx) = &self.get_stop_signal_tx {
            stop_tx.send(()).await.unwrap();
        } else {
            let stop_tx = self.observe_stop_signal_tx.take().unwrap();
            stop_tx.send(ObserveMessage::Terminate).unwrap();
        }

        self.join_handle.take().unwrap().await.unwrap()
        // match (&self.get_stop_signal_tx, &mut self.observe_stop_tx) {
        //     (None, Some(stop_tx)) => {
        //         if let Err(e) = stop_tx.send(ObserveMessage::Terminate) {
        //             warn!("stop send msg err:{:?}", e);
        //         }

        //         // self.token_manager
        //         //     .as_ref()
        //         //     .unwrap()
        //         //     .lock()
        //         //     .await
        //         //     .release(self.token.take().unwrap());
        //     }
        //     (Some(stop_tx), None) => {
        //         stop_tx.send(()).await.unwrap();
        //     }
        //     _ => unreachable!(),
        // }
    }
}
