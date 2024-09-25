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
    stop_signal_tx: mpsc::Sender<()>,
    join_handle: Option<JoinHandle<(DataConf, mpsc::Receiver<()>, mpsc::Receiver<MessageBatch>)>>,
    pub value: Arc<RwLock<serde_json::Value>>,
    ts: Arc<AtomicU64>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

impl Data {
    pub fn new(conf: DataConf) -> Self {
        let (stop_signal_tx, stop_signal_rx) = mpsc::channel(1);
        let (mb_tx, mb_rx) = mpsc::channel(16);

        let value = Arc::new(RwLock::new(serde_json::Value::Null));
        let ts = Arc::new(AtomicU64::new(0));
        let join_handle = Self::event_loop(conf, value.clone(), ts.clone(), stop_signal_rx, mb_rx);

        Self {
            stop_signal_tx,
            value,
            ts,
            join_handle: Some(join_handle),
            mb_tx,
        }
    }

    pub fn validate_conf(_conf: &DataConf) -> HaliaResult<()> {
        Ok(())
    }

    fn event_loop(
        conf: DataConf,
        value: Arc<RwLock<serde_json::Value>>,
        ts: Arc<AtomicU64>,
        mut stop_signal_rx: mpsc::Receiver<()>,
        mut mb_rx: mpsc::Receiver<MessageBatch>,
    ) -> JoinHandle<(DataConf, mpsc::Receiver<()>, mpsc::Receiver<MessageBatch>)> {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.recv() => {
                        return (conf, stop_signal_rx, mb_rx);
                    }

                    mb = mb_rx.recv() => {
                        if let Some(mb) = mb {
                            Self::handle_messsage_batch(mb, &conf, &value, &ts).await;
                        }
                    }
                }
            }
        })
    }

    async fn handle_messsage_batch(
        mut mb: MessageBatch,
        conf: &DataConf,
        value: &Arc<RwLock<serde_json::Value>>,
        ts: &Arc<AtomicU64>,
    ) {
        if let Some(msg) = mb.take_one_message() {
            match msg.get(&conf.field) {
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

    pub async fn update(&mut self, _old_conf: DataConf, new_conf: DataConf) {
        let (_, stop_signal_rx, mb_rx) = self.stop().await;
        let join_handle = Self::event_loop(
            new_conf,
            self.value.clone(),
            self.ts.clone(),
            stop_signal_rx,
            mb_rx,
        );
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) -> (DataConf, mpsc::Receiver<()>, mpsc::Receiver<MessageBatch>) {
        self.stop_signal_tx.send(()).await.unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }
}
