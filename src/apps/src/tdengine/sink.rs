use std::sync::Arc;

use common::constants::CHANNEL_SIZE;
use message::MessageBatch;
use taos::Taos;
use tokio::{
    select,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use types::apps::tdengine::{SinkConf, TDengineConf};

use super::new_tdengine_client;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: mpsc::Sender<MessageBatch>,
}

pub struct JoinHandleData {
    taos: Taos,
    stop_signal_rx: watch::Receiver<()>,
    mb_rx: mpsc::Receiver<MessageBatch>,
}

impl Sink {
    pub async fn new(conf: SinkConf, tdengine_conf: Arc<TDengineConf>) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = mpsc::channel(CHANNEL_SIZE);
        let taos = new_tdengine_client(&tdengine_conf, &conf).await;
        let join_handle_data = JoinHandleData {
            taos,
            stop_signal_rx,
            mb_rx,
        };

        let join_handle = Self::event_loop(join_handle_data);

        Self {
            stop_signal_tx,
            join_handle: Some(join_handle),
            mb_tx,
        }
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }
                    Some(mb) = join_handle_data.mb_rx.recv() => {
                    }
                }
            }
        })
    }

    fn handle_message_batch(conf: &SinkConf, taos: &Taos, mb: MessageBatch) {
        // INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update_conf() {}

    pub async fn update_tdengine_conf() {}
}
