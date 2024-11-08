use std::sync::Arc;

use common::{
    error::HaliaResult,
    sink_message_retain::{self, SinkMessageRetain},
};
use tracing::{debug, warn};
use message::RuleMessageBatch;
use schema::Encoder;
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch,
    },
    task::JoinHandle,
};
use types::apps::websocket::{AppConf, SinkConf};

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    join_handle: Option<JoinHandle<JoinHandleData>>,
    pub mb_tx: UnboundedSender<RuleMessageBatch>,
}

pub struct JoinHandleData {
    pub app_conf: Arc<AppConf>,
    pub sink_conf: SinkConf,
    pub encoder: Box<dyn Encoder>,
    pub message_retainer: Box<dyn SinkMessageRetain>,
    pub stop_signal_rx: watch::Receiver<()>,
    pub mb_rx: UnboundedReceiver<RuleMessageBatch>,
}

impl Sink {
    pub fn validate_conf(_conf: &SinkConf) -> HaliaResult<()> {
        Ok(())
    }

    pub async fn new(app_conf: Arc<AppConf>, sink_conf: SinkConf) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();
        let encoder = schema::new_encoder(&sink_conf.encode_type, &sink_conf.schema_id)
            .await
            .unwrap();

        let message_retainer = sink_message_retain::new(&sink_conf.message_retain);
        let join_handle_data = JoinHandleData {
            app_conf,
            sink_conf,
            encoder,
            message_retainer,
            stop_signal_rx,
            mb_rx,
        };

        let join_handle = Self::event_loop(join_handle_data);

        Self {
            mb_tx,
            stop_signal_tx,
            join_handle: Some(join_handle),
        }
    }

    fn event_loop(mut join_handle_data: JoinHandleData) -> JoinHandle<JoinHandleData> {
        let mut err = false;
        tokio::spawn(async move {
            loop {
                select! {
                    _ = join_handle_data.stop_signal_rx.changed() => {
                        return join_handle_data;
                    }

                    Some(mb) = join_handle_data.mb_rx.recv() => {
                        let mb = mb.take_mb();
                        if !err {
                            match join_handle_data.encoder.encode(mb) {
                                Ok(data) => {
                                    debug!("data: {:?}", data);
                                }
                                Err(e) => warn!("{:?}", e),
                            }

                        } else {
                            join_handle_data.message_retainer.push(mb);
                        }
                    }
                }
            }
        })
    }

    pub async fn update_conf(
        &mut self,
        _old_conf: SinkConf,
        new_conf: SinkConf,
    ) -> HaliaResult<()> {
        let mut join_handle_data = self.stop().await;
        join_handle_data.sink_conf = new_conf;
        Self::event_loop(join_handle_data);

        Ok(())
    }

    pub async fn stop(&mut self) -> JoinHandleData {
        self.stop_signal_tx.send(()).unwrap();
        self.join_handle.take().unwrap().await.unwrap()
    }

    pub async fn update_app_conf(&mut self, app_conf: Arc<AppConf>) {
        let mut join_handle_data = self.stop().await;
        join_handle_data.app_conf = app_conf;
        Self::event_loop(join_handle_data);
    }

    pub fn get_txs(&self, cnt: usize) -> Vec<UnboundedSender<RuleMessageBatch>> {
        let mut txs = vec![];
        for _ in 0..cnt {
            txs.push(self.mb_tx.clone());
        }
        txs
    }
}
