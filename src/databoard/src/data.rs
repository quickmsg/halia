use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use common::error::HaliaResult;
use message::MessageBatch;
use tokio::{
    select,
    sync::{mpsc, RwLock},
    task::JoinHandle,
};
use tracing::error;
use types::databoard::{DataConf, SearchDatasRuntimeResp};

pub struct Data {
    conf: DataConf,

    stop_signal_tx: mpsc::Sender<()>,
    join_handle: Option<JoinHandle<(mpsc::Receiver<()>, mpsc::Receiver<MessageBatch>)>>,
    pub value: Arc<RwLock<serde_json::Value>>,
    ts: Arc<AtomicU64>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Data {
    pub async fn new(conf: DataConf) -> HaliaResult<Self> {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let mut data = Data {
            conf,
            mb_tx,
            stop_signal_tx,
            value: Arc::new(RwLock::new(serde_json::Value::Null)),
            ts: Arc::new(AtomicU64::new(0)),
            join_handle: None,
        };

        data.event_loop(stop_signal_rx, mb_rx).await;

        Ok(data)
    }

    pub fn validate_conf(_conf: &DataConf) -> HaliaResult<()> {
        Ok(())
    }

    async fn event_loop(
        &mut self,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) {
        let field = self.conf.field.clone();
        let value = self.value.clone();
        let ts = self.ts.clone();
        let join_handle = tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (stop_signal_rx, mb_rx);
                    }

                    mb = mb_rx.recv() => {
                        if let Some(mb) = mb {
                            Self::handle_messsage_batch(mb, &field, &value, &ts).await;
                        }
                    }
                }
            }
        });
        self.join_handle = Some(join_handle);
    }

    async fn handle_messsage_batch(
        mut mb: MessageBatch,
        field: &String,
        value: &Arc<RwLock<serde_json::Value>>,
        ts: &Arc<AtomicU64>,
    ) {
        if let Some(msg) = mb.take_one_message() {
            match msg.get(field) {
                Some(v) => {
                    *value.write().await = v.clone().into();
                    match SystemTime::now().duration_since(UNIX_EPOCH) {
                        Ok(n) => ts.store(n.as_secs(), Ordering::SeqCst),
                        Err(e) => error!("{}", e),
                    }
                }
                None => {}
            }
        }
    }

    pub async fn read(&self) -> SearchDatasRuntimeResp {
        SearchDatasRuntimeResp {
            value: self.value.read().await.clone(),
            ts: self.ts.load(Ordering::SeqCst),
        }
    }

    pub async fn update(&mut self, old_conf: DataConf, new_conf: DataConf) {
        if new_conf != old_conf {
            self.conf = new_conf;
            self.stop_signal_tx.send(()).await.unwrap();
            let (stop_signal_rx, mb_rx) = self.join_handle.take().unwrap().await.unwrap();
            self.event_loop(stop_signal_rx, mb_rx).await;
        }
    }

    pub async fn stop(&mut self) {
        self.stop_signal_tx.send(()).await.unwrap();
        self.join_handle = None;
    }
}
