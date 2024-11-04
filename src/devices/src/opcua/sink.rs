use std::sync::Arc;

use anyhow::Result;
use message::{MessageBatch, RuleMessageBatch};
use opcua_protocol::{
    client::Session,
    types::{DataValue, UAString, WriteValue},
};
use tokio::{
    select,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        watch, RwLock,
    },
};
use tracing::warn;
use types::devices::device::opcua::SinkConf;

use super::transfer_node_id;

pub struct Sink {
    stop_signal_tx: watch::Sender<()>,
    pub mb_tx: UnboundedSender<RuleMessageBatch>,
}

impl Sink {
    pub fn validate_conf(_conf: SinkConf) -> Result<()> {
        Ok(())
    }

    pub fn new(opcua_client: Arc<RwLock<Option<Arc<Session>>>>, conf: SinkConf) -> Self {
        let (stop_signal_tx, stop_signal_rx) = watch::channel(());
        let (mb_tx, mb_rx) = unbounded_channel();
        Self::event_loop(conf, stop_signal_rx, mb_rx);
        Self {
            stop_signal_tx,
            mb_tx,
        }
    }

    fn event_loop(
        conf: SinkConf,
        mut stop_signal_rx: watch::Receiver<()>,
        mut mb_rx: UnboundedReceiver<RuleMessageBatch>,
    ) {
        tokio::spawn(async move {
            loop {
                select! {
                    _ = stop_signal_rx.changed() => {
                        break;
                    }
                    Some(mb) = mb_rx.recv() => {
                        // Self::handle_mb(&conf, &opcua_client, mb).await;
                    }
                }
            }
        });
    }

    async fn handle_mb(
        conf: &SinkConf,
        opcua_client: &Arc<RwLock<Option<Arc<Session>>>>,
        mut mb: MessageBatch,
    ) {
        let msg = match mb.take_one_message() {
            Some(msg) => msg,
            None => return,
        };

        let value = match msg.get(&conf.field) {
            Some(value) => value,
            None => return,
        };

        let data_value = match value {
            message::MessageValue::Null => todo!(),
            message::MessageValue::Boolean(b) => DataValue::from(*b),
            message::MessageValue::Int64(i64) => DataValue::from(*i64),
            message::MessageValue::Float64(_) => todo!(),
            message::MessageValue::String(_) => todo!(),
            message::MessageValue::Bytes(vec) => todo!(),
            message::MessageValue::Array(vec) => todo!(),
            message::MessageValue::Object(hash_map) => todo!(),
        };

        let node_id = transfer_node_id(&conf.node_id);
        let nodes_to_write = vec![WriteValue {
            node_id,
            attribute_id: 13,
            index_range: UAString::null(),
            value: data_value,
        }];

        match opcua_client.read().await.as_ref() {
            Some(client) => {
                if let Err(e) = client.write(nodes_to_write.as_slice()).await {
                    warn!("Failed to write to opcua: {:?}", e);
                }
            }
            None => return,
        };
    }

    pub async fn update_conf(&mut self, old_conf: SinkConf, new_conf: SinkConf) {
        todo!()
    }

    pub async fn stop(&mut self) {
        if let Err(e) = self.stop_signal_tx.send(()) {
            warn!("Failed to send stop signal: {:?}", e);
        }
    }
}
