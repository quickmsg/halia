use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use common::error::HaliaResult;
use message::{MessageBatch, RuleMessageBatch};
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch, RwLock,
    },
    task::JoinHandle,
};
use tracing::error;
use types::databoard::{DataConf, SearchDatasRuntimeResp};

pub struct Data {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub value: Arc<RwLock<serde_json::Value>>,
    ts: Arc<AtomicU64>,
    pub mb_tx: UnboundedSender<RuleMessageBatch>,
}

pub struct JoinHandleData {
    pub conf: DataConf,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: UnboundedReceiver<RuleMessageBatch>,
    pub value: Arc<RwLock<serde_json::Value>>,
    pub ts: Arc<AtomicU64>,
}

impl Data {
    pub fn new(conf: DataConf) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();

        let value = Arc::new(RwLock::new(serde_json::Value::Null));
        let ts = Arc::new(AtomicU64::new(0));
        let join_handle_data = JoinHandleData {
            conf,
            stop_signal_rx,
            mb_rx,
            value: value.clone(),
            ts: ts.clone(),
        };
        let join_handle = Self::event_loop(join_handle_data);

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

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    Some(mb) = join_handle_data.mb_rx.recv() => {
                        Self::handle_messsage_batch(mb.take_mb(), &join_handle_data.conf, &join_handle_data.value, &join_handle_data.ts).await;
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
            ts: self.ts.load(Ordering::Relaxed),
        }
    }

    pub async fn update(&mut self, _old_conf: DataConf, new_conf: DataConf) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.conf = new_conf;
        let join_handle = Self::event_loop(join_handle_data);
        self.join_handle = Some(join_handle);
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }
}
